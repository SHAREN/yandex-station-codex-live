# Установка с Codex OAuth без отдельного API-ключа

> Это инструкция для опытного владельца root-доступной Яндекс Станции Lite 1; **не запускайте на production-бинарнике станции до завершения тестирования исходника**.

## 1. CLIProxyAPI

Установите и настройте сторонний CLIProxyAPI отдельно. Подключите свою учётную запись Codex через **штатный OAuth-flow этого прокси**. Убедитесь, что конечная точка `/v1/realtime/calls` принимает SDP offer и возвращает SDP answer для `gpt-live-1-codex`. Сам мост не умеет получать/хранить OAuth и не подменяет модель.

В отличие от стандартного OpenAI Platform API ключ `OPENAI_API_KEY` не используется; фактическая работоспособность через подписку Codex не гарантирована документацией OpenAI и зависит от версии/провайдера прокси.

## 2. VPS

Используйте Python **3.12** (в мосте `audioop`). Установите `libopus0`, `ffmpeg` для тестов, создайте venv и выполните `pip install -r bridge/requirements.txt`.

Скопируйте `deploy/environment.example` во **внешний** root-only `/etc/yandex-station-codex-live.env`, исправьте URL локального CLIProxyAPI. Не вставляйте токены в Git. Для systemd см. `deploy/room-codex-live.service` (пути при необходимости изменить под установку).

В Nginx или другом reverse proxy включите TLS, Basic Auth и лимит подключений для `/room-gpt-live/ws`, образец `deploy/nginx-location.conf.example`. Не выставляйте локальный порт 18771 наружу. Проверьте `nginx -t`, затем `systemctl status room-codex-live`.

## 3. Home Assistant Pipecat

Скопируйте `ha/room_gpt_live_station.py` в модуль своего Pipecat Assist Proxy. В существующем WebSocket-маршруте `/api/station/lite/live`, **после принятия соединения**, выбирайте GPT Live только при `input_select.komnata_assistent == 'GPT Live'`. Пример:

```python
from app.room_gpt_live_station import is_selected, relay_station

if await is_selected(
    supervisor=SUPERVISOR_URL,
    token=os.getenv("SUPERVISOR_TOKEN", ""),
):
    if await relay_station(
        websocket,
        url=os.environ["ROOM_GPT_LIVE_URL"],
        password=os.environ["ROOM_GPT_LIVE_PASSWORD"],
    ):
        return
# Otherwise hand over to the existing stock/fallback flow.
```

Включите GET `/api/station/lite/codex-mode` с `ha/codex_mode.py`; эта точка сообщает `mode=codex` **только при GPT Live** и `mode=alice` либо `quasar` в остальных случаях, чтобы Go-агент никогда не путал модели.

```python
from app.codex_mode import station_mode
@app.get("/api/station/lite/codex-mode")
async def station_codex_mode():
    return await station_mode(SUPERVISOR_URL, os.getenv("SUPERVISOR_TOKEN", ""))
```

Не создавайте второй дублирующий WebSocket route: вставляйте вызов в уже существующий.

## 4. Станция (кандидат, НЕ развёрнут)

Требуются root, рабочий Quasar Bridge и штатный VQE `vqe_0`. Go-код `station/main.go` содержит адаптацию контроллера с выбором `codex`, но **её физический запуск пока не проверен**.

Настройте `STATION_CODEX_MODE_URL` и `STATION_CODEX_WS_URL` на адрес HA в домашней сети. Сборка требует современного Go toolchain и ARM64-библиотек; сначала `cd station && go test ./...`, затем кросс-компиляция для конкретной root-среды и проверка зависимостей. Нельзя просто подменить старый бинарник без резервной копии, контроля SHA и разрешённой процедуры отката.

## 5. Проверки

Сначала синтетический голос **без колонки** (`tests/test_synthetic.py` и собственный PCM). После проверки TLS/HA, режима и восстановимости — короткий физический тест. Если штатная Алиса озвучивает «произошла ошибка» после ответа GPT, это может быть старый параллельный VoiceInput; исследуйте Quasar Bridge/maind тайм-аут 12 секунд, а не глушите микрофон.

Подробности: [TESTING](TESTING.md).
