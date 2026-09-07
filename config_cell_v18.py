# ============================================================
# CELL 1 — CONFIGURATION
# ============================================================
# CONFIG_VERSION: bump this (and the filename) on every future edit,
# so a stale-cell mismatch shows up immediately in the printed banner
# below instead of silently running old code.
CONFIG_VERSION = "v18"

# ------------------------------------------------------------
# 0. СКОУП ЭТОГО ПРОГОНА (по буквальному ТЗ, не по нашему более
#    широкому пайплайну на полную 16-сетовую коллекцию)
# ------------------------------------------------------------
# ТЗ требует 1 сет (например «Кино коллекция») без золотых карт, в
# 3-5 вариантах на выбор продюсера. Для сдачи вызывается НЕ
# generate_collection()/render_collection() (это вся коллекция целиком,
# 16 сетов, 160 карт — избыточно для этого ТЗ), а обёртка
# render_set_variants("Кино коллекция") из card_pipeline.py (v30+) —
# она уже использует ORCHESTRATOR_NUM_VARIANTS/ORCHESTRATOR_CARDS_PER_SET
# ниже (3 варианта x 10 карт = 30 карт на рендер, без золота — это уже
# гарантировано на уровне generate_set_concepts, категория 5 туда не
# проходит).
# validated_cards (захардкоженные char_director_01/prop_3dglasses_01)
# в этот прогон НЕ включаются — они остаются отдельной ручной
# диагностикой категории 5/gold (см. TECHNICAL_SPEC §0), не частью
# сдаточного набора.
# v13: переход на платный тир. v16: ВОЗВРАТ на бесплатный тир (по
# запросу — платный пока отложен, не по техническим причинам). Модели
# и щадящие интервалы ниже — снова free-tier, обкатанные на реальных
# прогонах (v27/v28). ORCHESTRATOR_MAX_TOKENS/VISION_MAX_TOKENS
# (v30-фикс) оставлены как есть — полезны независимо от тира, не
# специфичны для платного.
# v17: IMAGE_ENGINE переключён на "openrouter" (платно, ~$0.039/картинка)
# — SDXL (local) уткнулся в устойчивый потолок именно на Category 1
# (floating isolated object): дубли объектов, читаемый текст-мусор,
# объект на видимой поверхности вместо парения — на popcorn_bucket,
# 3d_glasses, movie_statuette, clapperboard подряд, несмотря на то что
# все эти три failure-паттерна УЖЕ были явно прописаны в NEGATIVE_PROMPT
# (см. card_pipeline.py) — то есть это не пробел в промпте, который
# ещё не закрыли, а воспроизводимый потолок конкретно голого SDXL 1.0
# base без LoRA/файнтюна под product-shot (подтверждено раньше — тот же
# промпт в Gemini даёт чистый результат с первой подачи, см.
# process_report §3.4). LLM-оркестратор/Vision QA остаются на
# бесплатном тире (v16) — платится только генерация изображений.

import os
from kaggle_secrets import UserSecretsClient

# Должно быть выставлено ДО первого импорта torch (torch импортируется
# лениво внутри card_pipeline.py при первом вызове local-движка) — иначе
# CUDA-аллокатор уже создаст контекст со старыми настройками. По логу:
# "13.81 GiB allocated... 508 MiB reserved but unallocated" — классическая
# фрагментация; сам PyTorch в тексте ошибки предлагал ровно это.
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

secrets = UserSecretsClient()

# ------------------------------------------------------------
# 1. OPENROUTER / VISION QA
# ------------------------------------------------------------
os.environ["VISION_API_KEY"] = secrets.get_secret("OPENROUTER_API_KEY")
os.environ["VISION_API_BASE_URL"] = "https://openrouter.ai/api/v1"

# Основная модель + fallback через запятую. При устойчивом 429/зависании
# на основной пайплайн переключается на следующую в списке.
# ИСТОРИЯ: был переход на платный тир (v13, см. process_report §2.6),
# сейчас откачен обратно на бесплатный (v16, по запросу, не по
# техническим причинам) — см. заметку в §0 СКОУП выше. Модели ниже
# обкатаны на реальных прогонах до перехода на платный тир.
# ИСПРАВЛЕНО: две попытки render_collection() подряд упирались в один и
# тот же "upstream_provider_shared_pool" 429 — это специфично для Google
# AI Studio (обе модели, основная и фоллбэк, были на нём). Переключаю на
# провайдеров, физически не разделяющих этот пул: Qwen (Alibaba) основная,
# Kimi (Moonshot) фоллбэк — два разных бэкенда, снижает риск коррелирующего
# отказа на общем узком месте.
# ИСПРАВЛЕНО (снова): Qwen и Kimi, добавленные ранее сегодня как "другой
# провайдер" для обхода затора на Google AI Studio, ОБА оказались 404
# "No endpoints found" уже к следующему прогону — не заняты, а физически
# сняты с роутинга. Источники по состоянию на вчера/сегодня подтверждают:
# у OpenRouter регулярные чистки бесплатного тира без предупреждения —
# буквально "never hard-wire a recurring mission to one :free endpoint".
# Возвращаюсь на gemma (тот самый 429 сегодня means "жива, но занята",
# а не "умерла") + добавляю третьего провайдера (Nvidia, другую модель
# внутри той же линейки Nemotron) как резерв, а не Qwen/Kimi заново.
# ВАЖНО: любой конкретный список здесь стареет за дни, не месяцы —
# перед долгим прогоном стоит свериться с https://openrouter.ai/models
# (фильтр free) на актуальность, а не доверять этому списку вслепую.
# У обоих VISION_MODEL_FALLBACKS один и тот же провайдер (Google AI
# Studio) — коррелированный 429-риск. Третьей живой бесплатной
# vision-модели с другого провайдера для реальной диверсификации на
# OpenRouter физически нет (проверено — см. card_pipeline v27) —
# поэтому вместо диверсификации ниже стоит повтор всей цепочки после
# паузы (VISION_UNAVAILABLE_RETRY_ROUNDS/_BACKOFF_SECONDS).
os.environ["VISION_MODEL"] = "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free"
os.environ["VISION_MODEL_FALLBACKS"] = "google/gemma-4-26b-a4b-it:free,google/gemma-4-31b-it:free"

os.environ["VISION_API_MIN_INTERVAL_SECONDS"] = "4.0"   # щадящий интервал под free-tier rate limit
os.environ["MAX_VISION_RETRIES"] = "1"        # retry на ОДНУ модель при сетевой ошибке
os.environ["VISION_API_TIMEOUT_SECONDS"] = "45"
# max_tokens: полезно независимо от тира (v30-фикс — реальный ответ
# вердикта QA стоит копейки, но без явного потолка модель заявляет свой
# дефолт и это может упереться в лимиты по-своему даже на free-tier).
os.environ["VISION_MAX_TOKENS"] = "1500"

# Один повтор всей цепочки при полном отказе за проход (см. v27) —
# на free-tier это по-прежнему в основном против коррелированного
# затора общего пула у одного провайдера (оба google/gemma), не против
# обычных сетевых сбоев, как на платном — поэтому пауза длиннее.
os.environ["VISION_UNAVAILABLE_RETRY_ROUNDS"] = "2"
os.environ["VISION_UNAVAILABLE_BACKOFF_SECONDS"] = "20.0"

# ------------------------------------------------------------
# 1.1. LLM ORCHESTRATOR (тема -> варианты сета, Stage 1, текст-only)
# ------------------------------------------------------------
# ORCHESTRATOR_API_BASE_URL / ORCHESTRATOR_API_KEY можно не задавать —
# по умолчанию используются те же значения, что и VISION_API_BASE_URL /
# VISION_API_KEY (обычно один и тот же OpenRouter-аккаунт). Явно
# переопределяем здесь, только если для оркестратора нужен другой ключ.
#
# ORCHESTRATOR_MODEL обязателен — своего дефолта в коде нет специально
# (бесплатные модели на OpenRouter часто переименовываются/выводятся
# из ротации без предупреждения). Нужна текстовая модель (не vision) с
# хорошей инструктируемостью и поддержкой JSON-ответов.
# ВОЗВРАТ НА БЕСПЛАТНЫЙ ТИР (v16, см. VISION_MODEL выше) — primary:
# крупная reasoning-модель Nvidia; фоллбэки — Google (gemma) и Z.ai
# (glm), плюс nemotron-omni повторно как ещё один рабочий вариант в
# конце цепочки (диверсификация по провайдерам, обкатана на реальных
# прогонах — см. card_pipeline v27/v28).
os.environ["ORCHESTRATOR_MODEL"] = "nvidia/nemotron-3-ultra-550b-a55b:free"
os.environ["ORCHESTRATOR_MODEL_FALLBACKS"] = "google/gemma-4-31b-it:free,z-ai/glm-5.2:free,nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free"

os.environ["ORCHESTRATOR_API_MIN_INTERVAL_SECONDS"] = "4.0"  # щадящий интервал под free-tier
os.environ["MAX_ORCHESTRATOR_RETRIES"] = "1"
os.environ["ORCHESTRATOR_API_TIMEOUT_SECONDS"] = "60"
# max_tokens (v30-фикс, см. VISION_MAX_TOKENS выше) — полезно независимо
# от тира: реальный ответ на 10 карт стоит ~1000-2000 токенов, без
# явного потолка модель заявляет свой дефолт (у некоторых — 65536), что
# может упереться в лимиты по-своему даже на free-tier.
os.environ["ORCHESTRATOR_MAX_TOKENS"] = "4000"

os.environ["ORCHESTRATOR_NUM_VARIANTS"] = "3"
os.environ["ORCHESTRATOR_CARDS_PER_SET"] = "10"
# Для generate_collection() (ТЗ §0.2) — сколько обычных тематических
# сетов строить помимо гранд-сета (по спеке коллекция = 15 обычных + 1
# гранд = 16 сетов).
os.environ["ORCHESTRATOR_NUM_REGULAR_SETS"] = "15"

# ------------------------------------------------------------
# 2. IMAGE GENERATION
# ------------------------------------------------------------
# IMAGE_ENGINE переключает движок генерации без правок в card_pipeline.py:
#   "pollinations" — бесплатный zimage, нестабильное качество.
#   "local"        — SDXL прямо на GPU этого ноутбука, бесплатно (в
#                     рамках недельной GPU-квоты Kaggle), но упирается в
#                     собственный потолок именно на Category 1 (floating
#                     isolated object) — устойчиво воспроизводится на
#                     разных объектах (шляпы, попкорн, очки, статуэтка),
#                     не лечится дальнейшей правкой промпта (см.
#                     process_report §2.6/§3.4 — тот же промпт в Gemini
#                     даёт чистый результат с первой подачи).
#   "openrouter"   — ПЛАТНЫЙ (v31): Gemini/GPT Image через тот же
#                     OpenRouter-аккаунт, что и Vision QA/оркестратор.
#                     ~$0.039/картинка по цене на момент добавления —
#                     свериться на openrouter.ai перед долгим прогоном.
os.environ["IMAGE_ENGINE"] = "openrouter"

os.environ["IMAGE_GEN_MODEL"] = "google/gemini-2.5-flash-image"
os.environ["IMAGE_GEN_MODEL_FALLBACKS"] = "openai/gpt-5-image-mini"
os.environ["IMAGE_GEN_API_TIMEOUT_SECONDS"] = "90"

# Не требует API-ключа для базового использования. Актуально только для
# IMAGE_ENGINE="pollinations" (не активен сейчас, см. выше).
os.environ["IMAGE_API_ENDPOINT"] = "https://image.pollinations.ai/prompt/"
os.environ["IMAGE_MODEL"] = "zimage"          # было flux — смена не дала сдвига по композиции
os.environ["IMAGE_WIDTH"] = "1024"
os.environ["IMAGE_HEIGHT"] = "1024"
os.environ["IMAGE_API_MIN_INTERVAL_SECONDS"] = "6.0"
os.environ["IMAGE_API_TIMEOUT_SECONDS"] = "60"

# ------------------------------------------------------------
# 2.1 LOCAL SDXL SETTINGS (used only when IMAGE_ENGINE="local")
# ------------------------------------------------------------
# Не активно при текущем IMAGE_ENGINE="openrouter" (см. выше) — оставлено
# как есть на случай возврата к бесплатному локальному движку.
# SDXL 1.0 base: CreativeML Open RAIL++-M, без ограничения по выручке
# (в отличие от SD 3.5 Community License, у которой лимит $1M/год) —
# безопаснее по умолчанию для коммерческого мобильного проекта. Перед
# продакшен-релизом всё равно стоит самостоятельно перечитать текст
# лицензии — это не юридическая консультация, а ориентир для выбора.
os.environ["LOCAL_IMAGE_MODEL"] = "stabilityai/stable-diffusion-xl-base-1.0"
os.environ["LOCAL_IMAGE_STEPS"] = "30"
os.environ["LOCAL_IMAGE_GUIDANCE_SCALE"] = "7.0"

# ------------------------------------------------------------
# 3. PIPELINE SETTINGS
# ------------------------------------------------------------
os.environ["MAX_QA_RETRIES"] = "2"            # регенераций после QA FAIL
os.environ["MAX_NETWORK_RETRIES"] = "3"       # ретраев на сетевом уровне image-API
os.environ["MAX_PARALLEL_CARDS"] = "1"        # ограничение GPU Kaggle (1 инференс за раз), не тарифа API
os.environ["PER_CARD_TIMEOUT_SECONDS"] = "600"  # потолок на всю карту целиком
# Circuit breaker: после стольких QA_Unavailable подряд пайплайн сам
# остановится, не тратя GPU-время на карты, которые всё равно не
# пройдут QA прямо сейчас (например, кончилась дневная бесплатная
# квота, или временный затор общего пула у провайдера).
os.environ["MAX_CONSECUTIVE_QA_UNAVAILABLE"] = "3"

# Абсолютные пути под /kaggle/working — переживают перезапуск kernel В
# ПРЕДЕЛАХ одной интерактивной Edit-сессии (в отличие от относительных
# путей, которые зависели от cwd процесса). НЕ переживают закрытие
# ноутбука и открытие через день без явного "Save & Run All (with
# outputs)" — для полной персистентности между сессиями нужен
# закоммиченный output или подключённый Kaggle Dataset.
os.environ["OUTPUT_DIR"] = "/kaggle/working/generated_assets"
os.environ["EXPORT_JSON_PATH"] = "/kaggle/working/configs/cards_export_data.json"
# ДОБАВЛЕНО (v14): отдельная папка только для карт, реально прошедших QA
# (status=="Ready") — OUTPUT_DIR копит все попытки (Failed_QA,
# QA_Unavailable, промежуточные), в READY_OUTPUT_DIR render_collection()/
# render_set_variants() копируют (не переносят) только финальные Ready.
os.environ["READY_OUTPUT_DIR"] = "/kaggle/working/final_cards"
# ДОБАВЛЕНО (v18, реальный баг — см. диалог): Stage-1 (концепты карт от
# LLM-оркестратора) теперь тоже резюмируем, отдельно от Stage-2
# (сам рендер, EXPORT_JSON_PATH). Без этого файла повторный вызов
# render_set_variants() с той же темой каждый раз генерировал ВСЕ
# варианты заново с нуля (новые id — LLM не детерминирован), а старые
# уже отрендеренные Ready-карты становились сиротами и копились в
# READY_OUTPUT_DIR вперемешку с несовместимыми по концепции прогонами.
os.environ["SET_VARIANTS_EXPORT_JSON_PATH"] = "/kaggle/working/configs/set_variants_export_data.json"

# ------------------------------------------------------------
# 4. CHECK CONFIGURATION
# ------------------------------------------------------------
print("=" * 60)
print("CONFIG_VERSION:", CONFIG_VERSION)
print("=" * 60)
print("VISION CONFIG")
print("=" * 60)
print("Primary          :", os.environ["VISION_MODEL"])
print("Fallback         :", os.environ["VISION_MODEL_FALLBACKS"])
print("Vision interval  :", os.environ["VISION_API_MIN_INTERVAL_SECONDS"])
print("Vision retries   :", os.environ["MAX_VISION_RETRIES"])
print("Vision timeout   :", os.environ["VISION_API_TIMEOUT_SECONDS"])
print("Vision unavail rounds  :", os.environ["VISION_UNAVAILABLE_RETRY_ROUNDS"])
print("Vision unavail backoff :", os.environ["VISION_UNAVAILABLE_BACKOFF_SECONDS"], "s")
print("Vision max_tokens      :", os.environ["VISION_MAX_TOKENS"])
print("Vision key set   :", bool(os.environ.get("VISION_API_KEY")))
print()
print("ORCHESTRATOR CONFIG (Stage 1: theme -> set variants)")
print("=" * 60)
print("Model            :", os.environ["ORCHESTRATOR_MODEL"])
print("Fallback         :", os.environ["ORCHESTRATOR_MODEL_FALLBACKS"])
print("Num variants     :", os.environ["ORCHESTRATOR_NUM_VARIANTS"])
print("Cards per set    :", os.environ["ORCHESTRATOR_CARDS_PER_SET"])
print("Regular sets     :", os.environ["ORCHESTRATOR_NUM_REGULAR_SETS"], "(+ 1 grand set)")
print("Max tokens       :", os.environ["ORCHESTRATOR_MAX_TOKENS"])
print()
print("IMAGE CONFIG")
print("=" * 60)
print("Engine           :", os.environ["IMAGE_ENGINE"])
print("Image interval   :", os.environ["IMAGE_API_MIN_INTERVAL_SECONDS"])
print("Image timeout    :", os.environ["IMAGE_API_TIMEOUT_SECONDS"])
if os.environ["IMAGE_ENGINE"] == "local":
    print("Local model      :", os.environ["LOCAL_IMAGE_MODEL"])
    print("Local steps      :", os.environ["LOCAL_IMAGE_STEPS"])
    print("Local guidance   :", os.environ["LOCAL_IMAGE_GUIDANCE_SCALE"])
elif os.environ["IMAGE_ENGINE"] == "openrouter":
    print("Gen model        :", os.environ["IMAGE_GEN_MODEL"])
    print("Gen fallback     :", os.environ["IMAGE_GEN_MODEL_FALLBACKS"])
    print("Gen timeout      :", os.environ["IMAGE_GEN_API_TIMEOUT_SECONDS"])
else:
    print("Image model      :", os.environ["IMAGE_MODEL"])
print()
print("PIPELINE CONFIG")
print("=" * 60)
print("MAX_QA_RETRIES        :", os.environ["MAX_QA_RETRIES"])
print("MAX_NETWORK_RETRIES   :", os.environ["MAX_NETWORK_RETRIES"])
print("MAX_PARALLEL_CARDS    :", os.environ["MAX_PARALLEL_CARDS"])
print("PER_CARD_TIMEOUT_SECONDS:", os.environ["PER_CARD_TIMEOUT_SECONDS"])
print("MAX_CONSECUTIVE_QA_UNAVAILABLE:", os.environ["MAX_CONSECUTIVE_QA_UNAVAILABLE"])
print("OUTPUT_DIR             :", os.environ["OUTPUT_DIR"])
print("READY_OUTPUT_DIR        :", os.environ["READY_OUTPUT_DIR"])
print("EXPORT_JSON_PATH        :", os.environ["EXPORT_JSON_PATH"])
print("SET_VARIANTS_EXPORT_JSON_PATH:", os.environ["SET_VARIANTS_EXPORT_JSON_PATH"])
print("=" * 60)
