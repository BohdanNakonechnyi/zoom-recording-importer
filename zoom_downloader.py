#!/usr/bin/env python3
"""
Zoom Recording Downloader — інтерактивний консольний додаток.
Запуск: python zoom_downloader.py
"""

import os
import sys
import time
import shutil
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.progress import (
    Progress, BarColumn, TextColumn,
    DownloadColumn, TransferSpeedColumn, TimeRemainingColumn, TaskProgressColumn,
)
from rich import box
from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator

load_dotenv()

console = Console()

ZOOM_API_BASE = "https://api.zoom.us/v2"
ZOOM_OAUTH_URL = "https://zoom.us/oauth/token"
MAX_DATE_RANGE_DAYS = 30


# ─── Утиліти ──────────────────────────────────────────────────────────────────

def format_size(size_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def format_duration(minutes: int) -> str:
    if minutes >= 60:
        return f"{minutes // 60}г {minutes % 60:02d}хв"
    return f"{minutes}хв"


def sanitize_filename(name: str) -> str:
    for ch in r'\/:*?"<>|':
        name = name.replace(ch, "_")
    return name.strip(". ") or "recording"


def split_date_range(from_date: str, to_date: str) -> list[tuple[str, str]]:
    """Розбиває діапазон дат на шматки ≤ 30 днів (ліміт Zoom API)."""
    fmt = "%Y-%m-%d"
    start = datetime.strptime(from_date, fmt)
    end = datetime.strptime(to_date, fmt)
    ranges = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=MAX_DATE_RANGE_DAYS - 1), end)
        ranges.append((current.strftime(fmt), chunk_end.strftime(fmt)))
        current = chunk_end + timedelta(days=1)
    return ranges


def render_bar(used: int, total: int, width: int = 36) -> str:
    """Малює текстову шкалу заповнення."""
    if total <= 0:
        return "[dim]" + "░" * width + "[/]"
    pct = min(used / total, 1.0)
    filled = int(pct * width)
    empty = width - filled
    color = "green" if pct < 0.70 else "yellow" if pct < 0.90 else "red"
    return f"[{color}]{'█' * filled}[/][dim]{'░' * empty}[/]"


# ─── Zoom API клієнт ──────────────────────────────────────────────────────────

class ZoomClient:
    def __init__(self):
        self.account_id = os.getenv("ZOOM_ACCOUNT_ID", "")
        self.client_id = os.getenv("ZOOM_CLIENT_ID", "")
        self.client_secret = os.getenv("ZOOM_CLIENT_SECRET", "")
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._check_credentials()

    def _check_credentials(self):
        missing = [
            name for name, val in [
                ("ZOOM_ACCOUNT_ID", self.account_id),
                ("ZOOM_CLIENT_ID", self.client_id),
                ("ZOOM_CLIENT_SECRET", self.client_secret),
            ]
            if not val or "твій_справжній" in val
        ]
        if missing:
            console.print(Panel(
                "[red bold]Не заповнені Zoom credentials у .env:[/]\n"
                + "\n".join(f"  • {m}" for m in missing)
                + "\n\n[dim]Отримати: Zoom App Marketplace → Server-to-Server OAuth[/]",
                title="[red]Помилка конфігурації[/]",
                border_style="red",
            ))
            sys.exit(1)

    def get_token(self) -> str:
        if self._token and time.time() < self._token_expiry:
            return self._token
        with console.status("[dim]Отримання OAuth token...[/]", spinner="dots"):
            resp = requests.post(
                ZOOM_OAUTH_URL,
                params={"grant_type": "account_credentials", "account_id": self.account_id},
                auth=(self.client_id, self.client_secret),
                timeout=15,
            )
        if resp.status_code == 401:
            console.print("[red]Помилка автентифікації.[/] Перевірте CLIENT_ID та CLIENT_SECRET у .env")
            sys.exit(1)
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 3600) - 60
        return self._token

    def _h(self) -> dict:
        return {"Authorization": f"Bearer {self.get_token()}"}

    def get_all_users(self) -> list[dict]:
        """
        Повертає список юзерів акаунту: [{id, email, display_name}, ...]
        Потребує scope: user:read:list_users:admin
        """
        users = []
        page_token = None
        while True:
            params: dict = {"page_size": 300, "status": "active"}
            if page_token:
                params["next_page_token"] = page_token
            resp = requests.get(
                f"{ZOOM_API_BASE}/users",
                headers=self._h(),
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            for u in data.get("users", []):
                first = u.get("first_name", "")
                last  = u.get("last_name", "")
                full  = f"{first} {last}".strip() or u.get("email", u["id"])
                users.append({
                    "id":           u["id"],
                    "email":        u.get("email", ""),
                    "display_name": full,
                })
            page_token = data.get("next_page_token")
            if not page_token:
                break
        return users

    def _get_quota_bytes(self) -> tuple[int, str]:
        """
        Намагається отримати квоту хмарного сховища різними endpoints.
        Повертає (байт, опис_помилки_або_"").
        """
        import json

        # Читаємо квоту з .env (ZOOM_STORAGE_QUOTA_GB=90)
        quota_gb = os.getenv("ZOOM_STORAGE_QUOTA_GB", "").strip()
        if quota_gb and quota_gb.replace(".", "").isdigit() and float(quota_gb) > 0:
            return int(float(quota_gb) * (1024 ** 3)), ""
        return 0, "Квота не задана — додайте ZOOM_STORAGE_QUOTA_GB=90 у .env файл"

    def _fetch_user_size(self, user_id: str, from_date: str, to_date: str) -> int:
        """Повертає сумарний розмір записів одного юзера (в байтах)."""
        size = 0
        meetings = self.list_recordings(from_date, to_date, user_id=user_id)
        for m in meetings:
            for f in m.get("recording_files", []):
                size += f.get("file_size", 0)
        return size

    def get_cloud_storage_info(self) -> dict:
        """
        Повертає загальне використання хмарного сховища по всіх юзерах акаунту.
        Потрібні scopes:
          - cloud_recording:read:list_user_recordings:admin  — записи юзерів
          - user:read:list_users:admin                       — список всіх юзерів
          - account:read:account_setting:admin               — для спроби отримати квоту
        Повертає {"used": int, "total": int, "users_checked": int, "errors": list}.
        """
        errors = []
        used = 0
        users_checked = 0

        # ── 1. Квота (паралельно з іншим) ──
        total, quota_err = self._get_quota_bytes()
        if quota_err:
            errors.append(quota_err)

        # ── 2. Список юзерів ──
        try:
            all_users = self.get_all_users()
        except requests.HTTPError as e:
            if e.response.status_code in (401, 403):
                errors.append("Список юзерів: потрібен scope user:read:list_users:admin")
            else:
                errors.append(f"Список юзерів: HTTP {e.response.status_code}")
            all_users = [{"id": "me", "email": "", "display_name": "me"}]
        except Exception as e:
            errors.append(f"Список юзерів: {e}")
            all_users = [{"id": "me", "email": "", "display_name": "me"}]

        # ── 3. Паралельне завантаження записів для всіх юзерів ──
        from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        to_date = datetime.now().strftime("%Y-%m-%d")

        with ThreadPoolExecutor(max_workers=min(5, len(all_users))) as executor:
            futures = {
                executor.submit(self._fetch_user_size, u["id"], from_date, to_date): u
                for u in all_users
            }
            for future in as_completed(futures):
                u = futures[future]
                try:
                    used += future.result()
                    users_checked += 1
                except requests.HTTPError as e:
                    errors.append(f"Записи {u['email'] or u['id']}: HTTP {e.response.status_code}")
                except Exception as e:
                    errors.append(f"Записи {u['email'] or u['id']}: {e}")

        if users_checked == 0:
            errors.append("Не вдалось отримати записи жодного користувача")

        return {"used": used, "total": total, "users_checked": users_checked, "errors": errors}

    def list_recordings(self, from_date: str, to_date: str, user_id: str = "me") -> list:
        all_meetings = []
        for chunk_from, chunk_to in split_date_range(from_date, to_date):
            page_token = None
            while True:
                params: dict = {"from": chunk_from, "to": chunk_to, "page_size": 30}
                if page_token:
                    params["next_page_token"] = page_token
                resp = requests.get(
                    f"{ZOOM_API_BASE}/users/{user_id}/recordings",
                    headers=self._h(),
                    params=params,
                    timeout=30,
                )
                if resp.status_code == 404:
                    return []
                resp.raise_for_status()
                data = resp.json()
                all_meetings.extend(data.get("meetings", []))
                page_token = data.get("next_page_token")
                if not page_token:
                    break
        return all_meetings

    def download_file(self, url: str, dest: Path, expected_size: int = 0) -> bool:
        token = self.get_token()
        resp = requests.get(f"{url}?access_token={token}", stream=True, timeout=60)
        resp.raise_for_status()

        total = int(resp.headers.get("content-length", expected_size or 0))
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")

        try:
            with Progress(
                TextColumn("  [cyan]{task.description}[/]"),
                BarColumn(bar_width=35),
                TaskProgressColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                console=console,
                transient=False,
            ) as progress:
                task = progress.add_task(dest.name[:38], total=total or None)
                with open(tmp, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)
                            progress.update(task, advance=len(chunk))

            actual = tmp.stat().st_size
            if total > 0 and actual != total:
                console.print(
                    f"  [red]✗ Верифікація:[/] очікувано {format_size(total)}, "
                    f"отримано {format_size(actual)}"
                )
                tmp.unlink(missing_ok=True)
                return False

            tmp.rename(dest)
            return True

        except Exception:
            tmp.unlink(missing_ok=True)
            raise


# ─── Спільні UI елементи ──────────────────────────────────────────────────────

def draw_header(subtitle: str = ""):
    console.clear()
    title = "[bold cyan]Zoom Recording Downloader[/]"
    if subtitle:
        title += f"\n[dim]{subtitle}[/]"
    console.print(Panel.fit(title, border_style="cyan", padding=(0, 3)))
    console.print()


def pause(msg: str = "Натисніть Enter щоб повернутись до меню..."):
    console.print()
    input(msg)


def ask_date_range() -> tuple[str, str]:
    today = datetime.now()
    period = inquirer.select(
        message="Виберіть період:",
        choices=[
            Choice("7",      "Останні 7 днів"),
            Choice("30",     "Останні 30 днів"),
            Choice("90",     "Останні 3 місяці"),
            Choice("180",    "Останні 6 місяців"),
            Choice("365",    "Останній рік"),
            Choice("custom", "Власний діапазон дат"),
        ],
        default="30",
    ).execute()

    if period == "custom":
        from_str = inquirer.text(
            message="Від (YYYY-MM-DD):",
            default=(today - timedelta(days=30)).strftime("%Y-%m-%d"),
            validate=lambda x: len(x) == 10 and x[4] == "-" and x[7] == "-",
            invalid_message="Формат: YYYY-MM-DD",
        ).execute()
        to_str = inquirer.text(
            message="До (YYYY-MM-DD):",
            default=today.strftime("%Y-%m-%d"),
            validate=lambda x: len(x) == 10 and x[4] == "-" and x[7] == "-",
            invalid_message="Формат: YYYY-MM-DD",
        ).execute()
        return from_str, to_str

    days = int(period)
    return (today - timedelta(days=days)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


ALL_FILE_TYPES = {"MP4", "M4A", "TRANSCRIPT", "CHAT", "CC"}


def ask_file_types() -> set:
    choice = inquirer.select(
        message="Типи файлів:",
        choices=[
            Choice("MP4",  "MP4        — тільки відео (найчастіше)"),
            Choice("all",  "Всі типи   — MP4 + M4A + TRANSCRIPT + CHAT + CC"),
            Choice("pick", "Вибрати вручну..."),
        ],
        default="MP4",
    ).execute()

    if choice == "all":
        return ALL_FILE_TYPES.copy()

    if choice == "MP4":
        return {"MP4"}

    # Вручну
    selected = inquirer.checkbox(
        message="Оберіть типи файлів:",
        choices=[
            Choice("MP4",        name="MP4        — відео запис",        enabled=True),
            Choice("M4A",        name="M4A        — тільки аудіо"),
            Choice("TRANSCRIPT", name="TRANSCRIPT — авто транскрипт"),
            Choice("CHAT",       name="CHAT       — чат повідомлення"),
            Choice("CC",         name="CC         — субтитри"),
        ],
        instruction="(пробіл — вибрати, Enter — підтвердити)",
        validate=lambda x: len(x) > 0,
        invalid_message="Оберіть хоча б один тип",
    ).execute()
    return set(selected)


def ask_output_dir() -> Path:
    default = str(Path.home() / "Downloads" / "ZoomRecordings")
    path_str = inquirer.text(
        message="Директорія збереження:",
        default=default,
    ).execute()
    p = Path(path_str).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


def fetch_all_recordings(client: ZoomClient, from_date: str, to_date: str) -> tuple[list, list]:
    """
    Паралельно збирає записи ВСІХ юзерів акаунту.
    Кожна зустріч отримує поле _user_label з іменем/email юзера.
    Повертає (список зустрічей, список помилок).
    """
    try:
        all_users = client.get_all_users()
    except requests.HTTPError:
        all_users = [{"id": "me", "email": "", "display_name": "me"}]
    except Exception:
        all_users = [{"id": "me", "email": "", "display_name": "me"}]

    all_meetings: list = []
    errors: list = []
    lock = threading.Lock()

    def fetch_one(user: dict):
        label = user["display_name"] or user["email"] or user["id"]
        try:
            meetings = client.list_recordings(from_date, to_date, user_id=user["id"])
            # Теґуємо кожну зустріч іменем юзера
            for m in meetings:
                m["_user_label"] = label
            return meetings, None
        except Exception as e:
            return [], f"{label}: {e}"

    with ThreadPoolExecutor(max_workers=min(5, len(all_users))) as executor:
        futures = {executor.submit(fetch_one, u): u for u in all_users}
        for future in as_completed(futures):
            meetings, err = future.result()
            with lock:
                all_meetings.extend(meetings)
                if err:
                    errors.append(err)

    return all_meetings, errors


MAX_FOLDER_NAME = 60   # символів для назви зустрічі у папці
MAX_PATH_LEN    = 240  # залишаємо запас до ліміту Windows 260


def safe_meeting_dir(output_dir: Path, date: str, topic: str) -> Path:
    """
    Структура: output_dir / YYYY-MM-DD / назва_зустрічі
    Обрізає назву якщо загальний шлях надто довгий (ліміт Windows 260).
    """
    topic_short = topic[:MAX_FOLDER_NAME]
    candidate = output_dir / date / topic_short
    while len(str(candidate)) > MAX_PATH_LEN and len(topic_short) > 8:
        topic_short = topic_short[:-5]
        candidate = output_dir / date / topic_short
    return candidate


def build_file_list(meetings: list, output_dir: Path, file_types: set) -> list:
    files = []
    for meeting in sorted(meetings, key=lambda m: m.get("start_time", ""), reverse=True):
        topic = sanitize_filename(meeting.get("topic", "Без назви"))
        date = (meeting.get("start_time") or "")[:10]
        meeting_dir = safe_meeting_dir(output_dir, date, topic)

        for rec in meeting.get("recording_files", []):
            ftype = (rec.get("file_type") or "").upper()
            if ftype not in file_types:
                continue
            if rec.get("status", "completed") != "completed":
                continue

            ext = (rec.get("file_extension") or ftype).lower()
            rec_type = sanitize_filename(rec.get("recording_type") or "recording")
            dest = meeting_dir / f"{rec_type}.{ext}"
            size = rec.get("file_size") or 0
            already_done = dest.exists() and (size == 0 or dest.stat().st_size == size)

            files.append({
                "url": rec["download_url"],
                "dest": dest,
                "size": size,
                "meeting": topic,
                "date": date,
                "done": already_done,
            })
    return files


# ─── Екрани ───────────────────────────────────────────────────────────────────

def screen_cloud_storage(client: ZoomClient):
    draw_header("☁  Хмарне сховище Zoom")

    with console.status("[dim]Отримання інформації про сховище...[/]", spinner="dots"):
        info = client.get_cloud_storage_info()

    used = info["used"]
    total = info.get("total", 0)

    # ── Шкала хмарного сховища ──
    errs = info.get("errors", [])
    users_checked = info.get("users_checked", 0)

    if total > 0:
        pct = min(used / total * 100, 100)
        free = total - used
        free_color = "green" if pct < 70 else "yellow" if pct < 90 else "red"
        bar_line = render_bar(used, total) + f"  [bold]{pct:.1f}%[/]"
        body = (
            f"  Використано:  [bold cyan]{format_size(used)}[/] / [white]{format_size(total)}[/]\n"
            f"  Вільно:       [{free_color}]{format_size(free)}[/]\n"
            f"  Юзерів:       {users_checked}\n\n"
            f"  {bar_line}"
        )
    else:
        bar_line = render_bar(used, max(used * 2, 1))
        body = (
            f"  Записи займають: [bold cyan]{format_size(used)}[/]  (за останній рік)\n"
            f"  Юзерів перевірено: {users_checked}\n\n"
            f"  {bar_line}  [dim]квота невідома[/]"
        )

    if errs:
        body += "\n\n" + "\n".join(f"  [yellow]⚠ {e}[/]" for e in errs)

    console.print(Panel(
        body,
        title="[cyan]☁  Хмарне сховище Zoom[/]",
        border_style="cyan",
        padding=(1, 2),
    ))

    # ── Шкала локального диску ──
    home_usage = shutil.disk_usage(Path.home())
    local_pct = home_usage.used / home_usage.total * 100
    local_bar = render_bar(home_usage.used, home_usage.total)

    console.print(Panel(
        f"  Використано:  [bold]{format_size(home_usage.used)}[/] / {format_size(home_usage.total)}\n"
        f"  Вільно:       [green]{format_size(home_usage.free)}[/]\n\n"
        f"  {local_bar}  [bold]{local_pct:.1f}%[/]",
        title=f"[cyan]💾  Локальний диск ({Path.home().anchor})[/]",
        border_style="cyan",
        padding=(1, 2),
    ))

    pause()


def screen_list_recordings(client: ZoomClient):
    draw_header("📋 Перегляд записів")

    from_date, to_date = ask_date_range()
    console.print()

    with console.status("[dim]Збір записів всіх юзерів...[/]", spinner="dots"):
        meetings, errs = fetch_all_recordings(client, from_date, to_date)

    if errs:
        for e in errs:
            console.print(f"[yellow]⚠ {e}[/]")

    if not meetings:
        console.print("[yellow]Записів не знайдено за вказаний період.[/]")
        pause()
        return

    total_files = sum(len(m.get("recording_files", [])) for m in meetings)
    total_size = sum(
        f.get("file_size", 0)
        for m in meetings
        for f in m.get("recording_files", [])
    )

    table = Table(
        title=f"Записи: {from_date} → {to_date}  |  {len(meetings)} зустрічей, {total_files} файлів",
        box=box.ROUNDED,
        header_style="bold cyan",
        show_footer=True,
    )
    table.add_column("Дата",       style="cyan", width=12, no_wrap=True)
    table.add_column("Юзер",       width=18, no_wrap=True, footer="")
    table.add_column("Зустріч",    footer=f"[dim]{len(meetings)} зустрічей[/]", no_wrap=False)
    table.add_column("Тривал.",    justify="right", width=9,  footer="")
    table.add_column("Файлів",     justify="center", width=7, footer=str(total_files))
    table.add_column("Розмір",     justify="right",  width=10, footer=f"[cyan]{format_size(total_size)}[/]")

    for m in sorted(meetings, key=lambda x: x.get("start_time", ""), reverse=True):
        date     = (m.get("start_time") or "")[:10]
        topic    = m.get("topic", "—")
        label    = m.get("_user_label", "")
        if not label:
            label = m.get("host_email", m.get("host_id", ""))
        # Показуємо тільки частину до @
        label = label.split("@")[0] if "@" in label else label
        label = label[:18]
        duration = m.get("duration", 0)
        files    = m.get("recording_files", [])
        size     = sum(f.get("file_size", 0) for f in files)
        size_str = format_size(size) if size > 0 else "[dim]0 B[/]"
        table.add_row(date, label, topic, format_duration(duration),
                      str(len(files)), size_str)

    console.print(table)
    pause()


def _try_download(client: ZoomClient, f: dict) -> str:
    """
    Спроба завантажити один файл.
    Повертає статус: 'ok' | 'fail:<причина>'
    """
    try:
        ok = client.download_file(f["url"], f["dest"], f["size"])
        return "ok" if ok else "fail:розмір не збігається"
    except requests.HTTPError as e:
        return f"fail:HTTP {e.response.status_code}"
    except Exception as e:
        return f"fail:{e}"


def screen_download(client: ZoomClient):
    draw_header("📥 Завантаження записів")

    # 1. Параметри пошуку
    from_date, to_date = ask_date_range()
    console.print()
    file_types = ask_file_types()
    console.print()
    output_dir = ask_output_dir()
    console.print()

    # 2. Отримання списку з УСІХ юзерів паралельно
    with console.status("[dim]Збір записів всіх юзерів...[/]", spinner="dots"):
        meetings, fetch_errors = fetch_all_recordings(client, from_date, to_date)

    if fetch_errors:
        for e in fetch_errors:
            console.print(f"[yellow]⚠ {e}[/]")

    if not meetings:
        console.print("[yellow]Записів не знайдено за вказаний період.[/]")
        pause()
        return

    files = build_file_list(meetings, output_dir, file_types)

    if not files:
        console.print(f"[yellow]Файлів типу {', '.join(file_types)} не знайдено.[/]")
        pause()
        return

    # 3. Інтерактивний вибір файлів
    choices = []
    for f in sorted(files, key=lambda x: (x["date"], x["meeting"]), reverse=True):
        icon = "[green]✓[/] " if f["done"] else "[blue]↓[/] "
        label = (
            f"{f['date']}  {f['meeting'][:35]:<35}  "
            f"{f['dest'].name:<22}  {format_size(f['size']):>9}"
        )
        choices.append(Choice(
            value=f,
            name=f"{icon}{label}",
            enabled=not f["done"],
        ))

    selected = inquirer.checkbox(
        message=f"Виберіть файли ({len(files)} знайдено):",
        choices=choices,
        instruction="(пробіл — вибрати/зняти, a — всі, Enter — підтвердити)",
        transformer=lambda x: f"{len(x)} файл(ів) вибрано",
    ).execute()

    if not selected:
        console.print("[yellow]Нічого не вибрано.[/]")
        pause()
        return

    to_download = [f for f in selected if not f["done"]]
    already_ok  = [f for f in selected if f["done"]]
    new_size = sum(f["size"] for f in to_download)

    # 4. Перевірка місця на диску
    console.print()
    usage = shutil.disk_usage(output_dir)
    enough = new_size <= usage.free
    disk_bar = render_bar(usage.used, usage.total)

    after_free = usage.free - new_size
    after_color = "green" if after_free > 1024**3 else "yellow" if after_free > 0 else "red"

    console.print(Panel(
        f"  {disk_bar}  [bold]{usage.used / usage.total * 100:.1f}%[/] використано\n\n"
        f"  Вільно зараз:        [{'green' if enough else 'red'}]{format_size(usage.free)}[/]\n"
        f"  Потрібно скачати:    [cyan]{format_size(new_size)}[/]\n"
        f"  Вільно після:        [{after_color}]{format_size(max(after_free, 0))}[/]"
        + ("  [red]⚠ НЕДОСТАТНЬО![/]" if not enough else ""),
        title=f"[cyan]💾  {output_dir}[/]",
        border_style="cyan" if enough else "red",
        padding=(1, 2),
    ))

    if not enough:
        console.print("[red bold]  Недостатньо місця на диску![/]")
        pause()
        return

    # 5. Підтвердження
    console.print()
    n_new = len(to_download)
    n_skip = len(already_ok)
    confirmed = inquirer.confirm(
        message=(
            f"Завантажити {n_new} нових файлів ({format_size(new_size)})"
            + (f", {n_skip} вже існує — пропустити" if n_skip else "")
            + f"  →  {output_dir}?"
        ),
        default=True,
    ).execute()

    if not confirmed:
        console.print("[yellow]Скасовано.[/]")
        pause()
        return

    # 6. Завантаження з retry
    console.print()
    # Статуси: "ok" | "retry_ok" | "fail" | "existed" | "verify_fail"
    results: list[dict] = []

    for i, f in enumerate(to_download, 1):
        console.print(
            f"[bold cyan][{i}/{len(to_download)}][/] "
            f"{f['date']}  [dim]{f['meeting'][:50]}[/]"
        )

        status = _try_download(client, f)

        if status == "ok":
            # Верифікація розміру
            actual = f["dest"].stat().st_size if f["dest"].exists() else 0
            expected = f["size"]
            if expected > 0 and actual != expected:
                status = f"verify_fail:{format_size(actual)} замість {format_size(expected)}"
            else:
                console.print(f"  [green]✓ Готово[/]  {format_size(actual)}\n")

        if status != "ok":
            reason = status.split(":", 1)[1] if ":" in status else status
            console.print(f"  [yellow]↻ Повтор через помилку:[/] {reason}")
            time.sleep(2)
            status2 = _try_download(client, f)
            if status2 == "ok":
                actual = f["dest"].stat().st_size if f["dest"].exists() else 0
                console.print(f"  [green]✓ Повтор успішний[/]  {format_size(actual)}\n")
                status = "retry_ok"
            else:
                reason2 = status2.split(":", 1)[1] if ":" in status2 else status2
                console.print(f"  [red]✗ Не вдалось:[/] {reason2}\n")
                status = "fail"

        results.append({**f, "status": status})

    # Додаємо вже існуючі
    for f in already_ok:
        actual = f["dest"].stat().st_size if f["dest"].exists() else 0
        expected = f["size"]
        verify = "existed" if (expected == 0 or actual == expected) else "existed_wrong_size"
        results.append({**f, "status": verify})

    # 7. Детальний звіт
    console.print()
    report_table = Table(
        title="Детальний звіт",
        box=box.ROUNDED,
        header_style="bold cyan",
        show_footer=False,
    )
    report_table.add_column("Стан", width=8, justify="center")
    report_table.add_column("Дата", style="cyan", width=12, no_wrap=True)
    report_table.add_column("Зустріч")
    report_table.add_column("Файл")
    report_table.add_column("Розмір", justify="right", width=10)

    counts = {"ok": 0, "retry_ok": 0, "existed": 0, "fail": 0, "other": 0}

    for r in sorted(results, key=lambda x: x["date"], reverse=True):
        s = r["status"]
        if s == "ok":
            icon, style = "✓", "green"
            counts["ok"] += 1
        elif s == "retry_ok":
            icon, style = "↻✓", "yellow"
            counts["retry_ok"] += 1
        elif s == "existed":
            icon, style = "═", "dim"
            counts["existed"] += 1
        elif s == "existed_wrong_size":
            icon, style = "⚠═", "yellow"
            counts["other"] += 1
        elif s == "fail":
            icon, style = "✗", "red"
            counts["fail"] += 1
        else:
            icon, style = "⚠", "yellow"
            counts["other"] += 1

        size_display = format_size(r["dest"].stat().st_size) if r["dest"].exists() else "—"
        report_table.add_row(
            f"[{style}]{icon}[/]",
            r["date"],
            r["meeting"][:38],
            r["dest"].name,
            size_display,
        )

    console.print(report_table)

    # 8. Підсумок
    has_errors = counts["fail"] > 0 or counts["other"] > 0
    total_downloaded = counts["ok"] + counts["retry_ok"]
    ok_size = sum(
        r["dest"].stat().st_size for r in results
        if r["status"] in ("ok", "retry_ok") and r["dest"].exists()
    )

    lines = []
    if counts["ok"]:
        lines.append(f"  [green]✓  Скачано:[/]          {counts['ok']} файл(ів)  ({format_size(ok_size)})")
    if counts["retry_ok"]:
        lines.append(f"  [yellow]↻✓ Скачано з повтором:[/] {counts['retry_ok']} файл(ів)")
    if counts["existed"]:
        lines.append(f"  [dim]═  Вже існували:[/]      {counts['existed']} файл(ів) (перевірено)")
    if counts["fail"]:
        lines.append(f"  [red]✗  Не вдалось:[/]        {counts['fail']} файл(ів)")
    if counts["other"]:
        lines.append(f"  [yellow]⚠  Попередження:[/]      {counts['other']} файл(ів)")

    console.print(Panel(
        "\n".join(lines),
        title="[bold]Підсумок завантаження[/]",
        border_style="red" if has_errors else "green",
        padding=(1, 2),
    ))

    pause()


# ─── Точка входу ──────────────────────────────────────────────────────────────

def render_main_storage_panel(client: ZoomClient, cached: dict | None) -> dict:
    """
    Малює компактну шкалу сховища на головному екрані.
    Якщо cached не None — використовує кешовані дані без нового запиту.
    Повертає dict з даними для кешування.
    """
    if cached is None:
        with console.status("[dim]Завантаження інфо про сховище...[/]", spinner="dots"):
            info = client.get_cloud_storage_info()
    else:
        info = cached

    used  = info.get("used", 0)
    total = info.get("total", 0)
    users = info.get("users_checked", 0)

    # ── Zoom cloud ──
    if total > 0:
        pct = min(used / total * 100, 100)
        free = total - used
        color = "green" if pct < 70 else "yellow" if pct < 90 else "red"
        bar = render_bar(used, total, width=28)
        zoom_line = (
            f"☁  Zoom:   {bar} [{color}]{pct:.0f}%[/]  "
            f"[cyan]{format_size(used)}[/] / {format_size(total)}  "
            f"[dim]вільно {format_size(free)}[/]"
        )
    else:
        bar = render_bar(used, max(used * 2, 1), width=28)
        zoom_line = (
            f"☁  Zoom:   {bar} [dim]квота невідома[/]  "
            f"[cyan]{format_size(used)}[/]  [dim]({users} юзерів)[/]"
        )

    # ── Локальний диск ──
    local = shutil.disk_usage(Path.home())
    lpct  = local.used / local.total * 100
    lcolor = "green" if lpct < 80 else "yellow" if lpct < 92 else "red"
    lbar  = render_bar(local.used, local.total, width=28)
    disk_line = (
        f"💾  Диск:   {lbar} [{lcolor}]{lpct:.0f}%[/]  "
        f"[dim]вільно[/] [{lcolor}]{format_size(local.free)}[/]"
    )

    console.print(Panel(
        f"{zoom_line}\n{disk_line}",
        border_style="dim cyan",
        padding=(0, 1),
    ))
    console.print()
    return info


def screen_update():
    """Оновлення скрипту з GitHub через git pull."""
    import subprocess
    draw_header("🔄 Оновлення скрипту")

    script_dir = Path(__file__).parent

    # ── Перевірка наявності git ──
    try:
        subprocess.run(["git", "--version"], capture_output=True, check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        console.print(Panel(
            "[red]git не знайдено.[/]\n"
            "Встанови Git: [cyan]https://git-scm.com/download[/]",
            border_style="red",
        ))
        pause()
        return

    # ── Перевірка що ми в git репозиторії ──
    result = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        capture_output=True, text=True, cwd=script_dir,
    )
    if result.returncode != 0:
        console.print(Panel(
            "[red]Папка не є git репозиторієм.[/]\n"
            "Переконайся що скрипт склонований через [cyan]git clone[/]",
            border_style="red",
        ))
        pause()
        return

    remote_url = result.stdout.strip()
    console.print(f"[dim]Репозиторій:[/] {remote_url}\n")

    # ── Поточна версія (останній коміт) ──
    current = subprocess.run(
        ["git", "log", "-1", "--format=%h  %s  (%cr)"],
        capture_output=True, text=True, cwd=script_dir,
    ).stdout.strip()
    console.print(f"[cyan]Поточна версія:[/] {current}\n")

    # ── Перевірка що є нові коміти ──
    console.print("[dim]Перевірка оновлень...[/]")
    subprocess.run(["git", "fetch"], capture_output=True, cwd=script_dir)

    behind = subprocess.run(
        ["git", "rev-list", "HEAD..origin/master", "--count"],
        capture_output=True, text=True, cwd=script_dir,
    ).stdout.strip()

    # Спробуємо також main якщо master не знайшло
    if not behind.isdigit():
        behind = subprocess.run(
            ["git", "rev-list", "HEAD..origin/main", "--count"],
            capture_output=True, text=True, cwd=script_dir,
        ).stdout.strip()

    commits_behind = int(behind) if behind.isdigit() else 0

    if commits_behind == 0:
        console.print(Panel(
            "[green]✓ У вас остання версія скрипту.[/]",
            border_style="green",
        ))
        pause()
        return

    # ── Показуємо що нового ──
    console.print(f"[yellow]Доступно нових комітів: {commits_behind}[/]\n")

    new_commits = subprocess.run(
        ["git", "log", "HEAD..origin/master", "--format=  • %s  (%cr)", "--no-merges"],
        capture_output=True, text=True, cwd=script_dir,
    ).stdout.strip()
    if not new_commits:
        new_commits = subprocess.run(
            ["git", "log", "HEAD..origin/main", "--format=  • %s  (%cr)", "--no-merges"],
            capture_output=True, text=True, cwd=script_dir,
        ).stdout.strip()

    if new_commits:
        console.print(Panel(
            new_commits,
            title="[cyan]Нові зміни[/]",
            border_style="cyan",
        ))
        console.print()

    confirmed = inquirer.confirm(
        message="Оновити скрипт?",
        default=True,
    ).execute()

    if not confirmed:
        console.print("[yellow]Скасовано.[/]")
        pause()
        return

    # ── git pull ──
    pull = subprocess.run(
        ["git", "pull"],
        capture_output=True, text=True, cwd=script_dir,
    )

    if pull.returncode != 0:
        console.print(Panel(
            f"[red]Помилка оновлення:[/]\n{pull.stderr.strip()}",
            border_style="red",
        ))
        pause()
        return

    console.print(f"[green]✓ Оновлено![/]\n{pull.stdout.strip()}\n")

    # ── Перевіряємо чи змінився requirements.txt ──
    changed_files = subprocess.run(
        ["git", "diff", "HEAD~1", "--name-only"],
        capture_output=True, text=True, cwd=script_dir,
    ).stdout.strip()

    if "requirements.txt" in changed_files:
        console.print("[yellow]requirements.txt змінився — оновлюємо залежності...[/]")
        pip = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "-q"],
            capture_output=True, text=True, cwd=script_dir,
        )
        if pip.returncode == 0:
            console.print("[green]✓ Залежності оновлені.[/]")
        else:
            console.print(f"[red]Помилка pip:[/] {pip.stderr.strip()}")

    console.print(Panel(
        "[green]✓ Скрипт оновлено.[/] Перезапусти програму щоб зміни вступили в силу.",
        border_style="green",
    ))
    pause()


def main():
    client = ZoomClient()
    storage_cache: dict | None = None   # кешуємо щоб не запитувати кожного разу

    while True:
        draw_header()
        storage_cache = render_main_storage_panel(client, storage_cache)

        action = inquirer.select(
            message="Виберіть дію:",
            choices=[
                Choice("storage",  "☁   Детально про сховище / оновити"),
                Choice("list",     "📋  Переглянути записи"),
                Choice("download", "📥  Завантажити записи"),
                Separator("─" * 30),
                Choice("update",   "🔄  Оновити скрипт з GitHub"),
                Choice("exit",     "❌  Вийти"),
            ],
            pointer="▶",
        ).execute()

        if action == "storage":
            storage_cache = None
            screen_cloud_storage(client)
        elif action == "list":
            screen_list_recordings(client)
        elif action == "download":
            screen_download(client)
            storage_cache = None
        elif action == "update":
            screen_update()
        elif action == "exit":
            console.print("\n[dim]До побачення![/]\n")
            break


if __name__ == "__main__":
    main()
