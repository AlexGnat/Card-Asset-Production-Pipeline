# ============================================================
# CARD ASSET PRODUCTION PIPELINE — v2 (fixed)
# ============================================================
#
# Исправления относительно v2:
# - MAX_QA_RETRIES, MAX_NETWORK_RETRIES теперь реально читаются из env
#   (раньше были захардкожены литералами, конфиг-ячейка их не влияла)
# - IMAGE_API_TIMEOUT_SECONDS / VISION_API_TIMEOUT_SECONDS реально
#   используются в timeout= вызовов requests (раньше был хардкод timeout=60)
# - убран POLLINATIONS_API_KEY: pollinations.ai не требует ключа для
#   базового использования, а копирование туда ключа OpenRouter было
#   просто путаницей без функционального смысла
# ============================================================
# Исправления в v27 (по факту реального прогона generate_collection() +
# render_collection() на 160 картах, см. лог в истории обсуждения):
# - perform_image_qa(): один повтор ВСЕЙ цепочки vision-моделей после
#   паузы, если все они недоступны за один проход — в логе видно, что
#   429 у обоих google/gemma-* коррелирован (оба на одном апстрим-
#   провайдере), но это было КРАТКОВРЕМЕННОЕ явление, не дневной лимит:
#   те же карты через несколько минут обрабатывались нормально. Раньше
#   такой всплеск сразу засчитывался как QA_Unavailable и три подряд
#   роняли весь пайплайн через circuit breaker, даже когда квота была
#   не исчерпана. Третьего провайдера для vision на бесплатном тире
#   сейчас физически нет (проверено) — это не то же самое упущение,
#   что было раньше с ORCHESTRATOR_MODEL_FALLBACKS.
# - perform_image_qa(): при невалидном ответе (например, отсутствующий
#   'choices') в лог теперь пишется и сырое тело ответа — раньше терялся
#   текст самого ответа модели, оставался только текст исключения.
# - Добавлен render_set_variants() — Stage 1+2 обёртка ДЛЯ ОДНОГО СЕТА
#   С ВАРИАНТАМИ (в отличие от generate_collection()/render_collection(),
#   которые строят вообще всю 16-сетовую коллекцию). Нужен для сдачи по
#   ТЗ: 1 сет, 3-5 вариантов на выбор продюсера, без золота — раньше
#   такого готового однострочного входа не было, приходилось вручную
#   разворачивать список вариантов перед run_production_pipeline.
# ============================================================
# Исправления в v28 (по факту реального лога вызова
# render_set_variants("Кино коллекция") — все 4 orchestrator-модели
# отвалились на ОДНОМ запросе с num_variants=3):
# - generate_set_concepts(): раньше все num_variants вариантов
#   запрашивались ОДНИМ HTTP-вызовом ("Propose exactly 3 different
#   candidate sets..."). Для num_variants=3 это стабильно валило всю
#   fallback-цепочку — primary упирался в hard-timeout (объём JSON
#   ~3x больше, генерация не укладывалась в 70с), фолбэки возвращали
#   4xx/пустые ответы. generate_collection() при этом ВСЕГДА просил по
#   1 варианту за вызов для каждого из 15 сетов — и именно этот размер
#   запроса подтверждённо работал в реальном прогоне. Теперь
#   generate_set_concepts() всегда делает N отдельных вызовов по 1
#   варианту (через новую _request_single_set_variant()), а не один
#   "толстый" вызов на все N сразу — дороже по числу запросов, но это
#   единственный размер запроса, который проверен на практике.
# - Сбой ОДНОГО варианта (после полного перебора fallback-моделей)
#   больше не роняет всю генерацию сета — по аналогии с missing_sets
#   в generate_collection(), пропущенный вариант логируется явно, а
#   остальные варианты генерируются как обычно.
# - Убрана тишина в логе при HTTP 4xx/429 от orchestrator-модели —
#   раньше в этом случае в логе не было НИКАКОГО сообщения между
#   "requesting" и "switching to fallback" (см. лог: gemma-4-31b и
#   glm-5.2 отваливались беззвучно), теперь причина логируется явно.
# - Убран хардкод "3" в системном промпте оркестратора ("each of the 3
#   proposed sets...") — был некорректен уже и раньше (generate_collection
#   всегда просил 1 вариант за вызов), а после перехода на "всегда 1
#   вариант за HTTP-вызов" стал вводить модель в заблуждение на каждом
#   запросе.
# ============================================================
# Исправления в v29 (по факту реального прогона render_set_variants и
# вопросов из диалога — "не могу найти готовые карточки", "почему 2
# варианта вместо 3"):
# - Найден и исправлен реальный баг с коллизией id между вариантами:
#   каждый вариант теперь отдельный независимый HTTP-вызов (см. v28) —
#   модель не видит id, использованные в ДРУГИХ вариантах того же сета,
#   и системный промпт гарантирует уникальность id только В ПРЕДЕЛАХ
#   одного ответа. Без этого фикса два варианта могли случайно назвать
#   разные карты одинаково (например, оба — "prop_camera_01"), а id —
#   это одновременно ключ resume/QA-истории И основа имени файла на
#   диске, так что коллизия тихо перезаписывала бы файл одной карты
#   файлом другой и путала их QA-историю. Теперь id всегда
#   префиксуется set_id варианта (который уже включает v-индекс) —
#   гарантия уникальности не зависит от того, повезёт ли с моделью.
#   ВАЖНО: это меняет id уже сгенерированных карт — resume не узнает
#   старые Ready-карты из прошлых прогонов под старыми (непрефиксован-
#   ными) id, они будут сгенерированы заново при следующем запуске.
# - Добавлена READY_OUTPUT_DIR (конфиг) + _export_ready_assets() —
#   после рендера карты со статусом Ready теперь автоматически
#   КОПИРУЮТСЯ (не переносятся, оригиналы остаются для resume) в
#   отдельную папку, без мусора из Failed_QA/QA_Unavailable/
#   промежуточных попыток. render_collection() и render_set_variants()
#   в конце печатают список [card_id] путь для каждого скопированного
#   файла — не нужно искать вручную по OUTPUT_DIR.
# Исправления в v30 (по факту реального лога — все 3 варианта отвалились
# с HTTP 402 "requires more credits... requested up to 65536 tokens,
# but can only afford N"):
# - Ни в perform_image_qa(), ни в _request_single_set_variant()/
#   generate_subtheme_names()/generate_grand_set_concepts() не был
#   выставлен max_tokens — модели заявляли свой дефолтный потолок
#   (у некоторых платных моделей — до 65536), и OpenRouter отказывал
#   запросу ЗАРАНЕЕ (402), если баланс не покрывает этот теоретический
#   максимум, даже когда реальный ответ (10 карт JSON / вердикт QA)
#   стоит на 1-2 порядка дешевле. Добавлены явные ORCHESTRATOR_MAX_TOKENS
#   (дефолт 4000) и VISION_MAX_TOKENS (дефолт 1500) — оба с запасом
#   выше реальной потребности, но на порядок ниже дефолтного потолка
#   моделей, так что предварительная проверка affordability на стороне
#   OpenRouter больше не должна упираться в баланс раньше времени.
#   Это снижает требуемый баланс для прогона, но НЕ отменяет
#   необходимость пополнить счёт — если баланс близок к нулю, 402
#   всё ещё возможен, просто порог станет реалистичным, а не
#   искусственно завышенным дефолтом модели.
# ============================================================
# Добавлено в v31 (по запросу — SDXL local упёрся в устойчивый потолок
# именно на Category 1, см. process_report §2.6/§3.4 и диалог):
# - Новый движок _generate_image_openrouter() (IMAGE_ENGINE="openrouter")
#   — Gemini/GPT Image через тот же OpenRouter-аккаунт, что и Vision QA/
#   оркестратор. Тот же bool-контракт (prompt, negative_prompt, filepath,
#   seed, max_retries, width, height) -> bool, что и у local/pollinations
#   — переключается одной строкой конфига, без правок в вызывающем коде
#   (process_card/generate_preview/diagnose_* не тронуты).
# - negative_prompt дописывается в сам промпт как "Avoid: ..." — у
#   мультимодальных Gemini/GPT Image нет отдельного API-канала для
#   negative prompt (в отличие от diffusion-моделей с CFG у SDXL).
# - seed передаётся в payload, но без гарантии, что провайдер его
#   учитывает (OpenRouter не документирует это для image-модалити) —
#   не критично: вариативность между попытками и так обеспечена самой
#   генерацией, в отличие от _generate_image_local, где seed — честный
#   diffusion-параметр.
# ============================================================
# Исправлено в v32 (реальный баг из диалога — накопленный "мусор" из
# разных прогонов в READY_OUTPUT_DIR, >10 разных предметов под одной
# темой вместо чистых 3×10 карт):
# - generate_set_concepts()/render_set_variants() до этого фикса НЕ
#   сохраняли Stage-1 результат вообще (концепты карт от LLM-
#   оркестратора) — только Stage-2 (сам рендер картинок) был
#   резюмируемым через EXPORT_JSON_PATH. Любой повторный вызов
#   (обрыв по дневной квоте, перезапуск ядра, что угодно) заново гонял
#   LLM за ВСЕМИ вариантами с нуля — с НОВЫМИ id карт (LLM не
#   детерминирован между вызовами), из-за чего старые уже
#   отрендеренные Ready-картинки становились сиротами: их id не
#   совпадали с id новых концептов, resume в run_production_pipeline
#   их не узнавал, а на диске копилась смесь карт из концептуально
#   несовместимых прогонов. generate_collection() эту же проблему уже
#   решала для полной коллекции через COLLECTION_EXPORT_JSON_PATH —
#   для одно-сетового пути (render_set_variants, добавлен позже, в
#   v28) аналогичного Stage-1 resume не было.
# - Добавлен SET_VARIANTS_EXPORT_JSON_PATH — кэш вариантов по (theme,
#   set_id_prefix), симметричный по духу COLLECTION_EXPORT_JSON_PATH.
#   При resume=True (дефолт) повторный вызов с той же темой
#   переиспользует уже сгенерированные варианты и достраивает только
#   недостающие индексы — не трогая LLM за уже готовое. Сохраняется
#   по готовности каждого варианта, не в конце (обрыв на #2 не теряет
#   #1). Несовпадение num_variants/cards_per_set с закэшированными —
#   кэш для этого ключа считается несовместимым и не подмешивается.
# ============================================================
# PIPELINE_FILE_VERSION: v1 — отдельный счётчик от заголовка выше (тот
# описывает более раннюю правку). Бампается на каждое следующее
# изменение этого файла + печатается при загрузке ячейки, чтобы
# несовпадение "что вставлено в ячейку" vs "что реально прислано"
# было видно сразу в выводе, а не всплывало через 10 минут отладки лога.

PIPELINE_FILE_VERSION = "v32"

import os
import re
import glob
import json
import time
import base64
import shutil
import hashlib
import logging
import tempfile
import threading
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED

import requests


# ============================================================
# 0.1 HARD WALL-CLOCK TIMEOUT WRAPPER
# ============================================================
# requests' параметр timeout= в редких случаях не спасает — если сетевой
# вызов зависает на более низком уровне (типичный кейс: getaddrinfo()
# при резолвинге DNS блокируется на уровне ОС и не проверяет ни таймаут,
# ни сигналы прерывания, поэтому даже KeyboardInterrupt/Kaggle Stop не
# может его прервать — помогает только Restart Session).
#
# Оборачиваем вызов в отдельный ДЕМОН-поток (daemon=True — принципиально:
# такой поток никогда не блокирует завершение процесса/kernel, даже если
# зависнет навсегда). Если за отведённое время ответа нет, отдаём
# управление обратно немедленно и считаем попытку проваленной; зависший
# поток просто остаётся жить в фоне, ничего не блокируя.

import threading as _threading


def _call_with_hard_timeout(fn, timeout_seconds, *args, **kwargs):
    result_box = {}

    def runner():
        try:
            result_box["value"] = fn(*args, **kwargs)
        except Exception as e:
            result_box["error"] = e

    t = _threading.Thread(target=runner, daemon=True)
    t.start()
    t.join(timeout_seconds)

    if t.is_alive():
        raise requests.exceptions.Timeout(
            f"Hard wall-clock timeout after {timeout_seconds}s "
            f"(underlying call may still be hanging in a background daemon thread)"
        )
    if "error" in result_box:
        raise result_box["error"]
    return result_box.get("value")




# ============================================================
# 0. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

log = logging.getLogger("card_pipeline")


# ============================================================
# 1. MASTER STYLE
# ============================================================

MASTER_STYLE = (
    "premium casual mobile game illustration, "
    "warm cheerful cartoon style, "
    "soft rounded shapes, "
    "bright saturated colors, "
    "gentle painterly shading, "
    "clean bold silhouettes, "
    "high visual clarity, "
    "strong focal point, "
    "polished match-3 mobile game production quality"
)
# ИСПРАВЛЕНО (найдено через diagnose_prompt_ablation): было ещё
# "friendly approachable character design, " — эта фраза уходит в КАЖДЫЙ
# промпт через STYLE-блок в build_prompt(), включая карты-предметы
# категории 1-2. Ablation показал прямую причинно-следственную связь:
# A_minimal (без STYLE) — просто нагромождение очков, без персонажей;
# как только подключается STYLE (уровень B) — тут же появляются
# персонажи в очках, и остаются на C/D. Слово "character" в общем
# стилевом блоке буквально просило персонажа на любой карте, включая
# изолированный объект. Для карт-персонажей (director и т.п.) это не
# нужно отдельно — их "character"-суть уже задаётся текстом самого
# concept/concept_visual_guidance, а не общим стилевым блоком.


# ============================================================
# 1.5 COMPEL WEIGHT SYNTAX STRIPPING (для не-SDXL движков)
# ============================================================
# НАЙДЕНО ПРИ РЕВЬЮ (gemini): concept_visual_guidance/concept_negative_terms
# используют compel-синтаксис вида "(текст)1.4" — это понимает ТОЛЬКО
# compel (для SDXL, IMAGE_ENGINE=local). Если IMAGE_ENGINE=pollinations,
# этот же текст летит как есть на чужой text encoder (CLIP/T5 у zimage/
# flux), который не умеет распарсить "(...)1.4" — скобки и число попадают
# в промпт как обычные символы, что реально может давать шум/артефакты.
# Раньше такого хвоста в тексте не было — веса появились позже (v10), и
# при разработке IMAGE_ENGINE проверялся только на local, эта комбинация
# не тестировалась. Снимаем веса ТОЛЬКО на pollinations-пути — для local
# они должны доходить до compel как есть.

_COMPEL_WEIGHT_RE = re.compile(r"\(([^()]*)\)\d+(?:\.\d+)?")


def _strip_compel_weights(text: str) -> str:
    """"(some phrase)1.4" -> "some phrase" — для движков без compel."""
    return _COMPEL_WEIGHT_RE.sub(r"\1", text)


# ============================================================
# 2. GLOBAL NEGATIVE PROMPT
# ============================================================

NEGATIVE_PROMPT = (
    "text, words, letters, numbers, typography, writing, "
    "captions, labels, signs, subtitles, logos, watermark, "
    "signature, barcode, QR code, gibberish, pseudo-writing, "
    "illegible symbols resembling text, "
    "trading card frame, card border with text box, rules text, "
    "stat block, mana cost symbol, "
    "papers, documents, clapperboard, script pages, sign board, "
    "photorealistic, photograph, photo, realistic photography, "
    "3D render, cinematic film still, live-action, hyperrealistic skin, "
    "film grain, DSLR photo, "
    "gritty, dark moody lighting, horror atmosphere, film noir, "
    "desaturated colors, harsh dramatic shadows, "
    "blurry, low resolution, noisy image, "
    "bad composition, cropped subject, "
    "extra limbs, malformed anatomy, duplicate objects, "
    "crowd, background people, unnecessary characters, "
    # ДОБАВЛЕНО: этот же "grid/catalog" паттерн, который мы долго давили
    # точечно только для одной карты (3D viewer/glasses), повторился на
    # ДРУГОМ объекте (шляпы) в реальной LLM-сгенерированной коллекции —
    # значит, это общая склонность SDXL к раскладке мелких аксессуаров
    # в узор/каталог, а не специфика одного конкретного существительного.
    # Поднято с уровня concept_negative_terms() на уровень глобального
    # негатива, чтобы защищать все карты, а не только две вручную
    # прописанные тестовые.
    "repeating pattern, seamless pattern, tiled pattern, wallpaper "
    "pattern, product catalog, catalog page, grid of objects, group of "
    "objects, collection of objects, rows of objects, columns of "
    "objects, array of items, multiple copies of the same object"
)


# ============================================================
# 3. CATEGORY VISUAL RULES
# ============================================================

CATEGORY_LABELS = {
    1: "Floating isolated object",
    2: "Object on simple surface + flat backdrop",
    3: "Object on realistic surface + studio environment",
    4: "Object in full realistic environment",
    5: "Gold character story scene",
}


CATEGORY_RULES = {

    # ИСПРАВЛЕНО (ТЗ §0.4): категория 1 потеряла два требования из
    # оригинального скрина "Общие правила категорий" при более раннем
    # сокращении под лимит длины URL — "объект без мелких деталей" и
    # альтернативный фон-паттерн (полосы/повторяющиеся объекты/световые
    # лучи), оставался только "simple solid-color or gradient". Оба
    # восстановлены.
    #
    # Категории 2-3 НЕ перепроверены на ту же регрессию — в моём
    # распоряжении нет оригинального скрина, чтобы свериться построчно
    # (в ТЗ §0.4 явно отмечено "перепроверить 2-3 на то же самое — не
    # проверялось систематически"). Если оригинальный текст ещё
    # доступен — стоит свериться отдельно, прежде чем считать вопрос
    # закрытым.
    # ИСПРАВЛЕНО (компромисс качество/срок, сознательно): "no cast
    # shadow" оказался самым частым поводом для QA FAIL по всей
    # коллекции на бесплатном SDXL 1.0 base без LoRA — модель почти
    # всегда оставляет лёгкую тень под "парящим" объектом, сколько ни
    # ретраить. Дано явное указание продюсеру/заказчику: лучше принять
    # этот один второстепенный дефект и получить результат в срок на
    # бесплатном движке, чем жечь попытки/квоту на недостижимой (для
    # этой конкретной модели) идеальной "нулевой тени". Мягкая тень БЕЗ
    # видимой поверхности — теперь допустима; видимая поверхность
    # (пол/стол/постамент) — по-прежнему запрещена, это уже настоящее
    # нарушение "floating isolated object", а не мелкий артефакт.
    1: (
        "Category 1: a simple object with no fine details, no small "
        "parts, no intricate surface texture — reads as a few large "
        "clean shapes. The object floats in mid-air with no visible "
        "ground, floor, table, or pedestal surface beneath it — a soft, "
        "subtle cast shadow directly under the object is acceptable (a "
        "minor, tolerated generation artifact), but the object must "
        "never appear to rest ON a visible surface. Background is "
        "EITHER a simple solid-color or gradient, OR a simple repeating "
        "pattern (stripes, repeated objects, or light rays). No "
        "environment, no secondary objects."
    ),

    2: (
        "Category 2: the object stands on a simple surface with a plain "
        "flat vertical backdrop directly behind it (solid color or "
        "gradient). No developed environment, no secondary objects."
    ),

    3: (
        "Category 3: the object stands on a realistic simple surface with "
        "a plain vertical studio backdrop behind it, realistic contact "
        "shadows and volumetric depth. Not a fully developed location."
    ),

    4: (
        "Category 4: the object is integrated into a fully realized "
        "realistic environment with recognizable spatial context, while "
        "remaining the clear focal point."
    ),

    # ИСПРАВЛЕНО (ТЗ §0.7): "Описание коллекций" явно требует "ироничную
    # эмоциональную ситуацию" для золотых карточек — было потеряно,
    # оставался нейтральный "one small clear narrative moment" без
    # требования иронии/юмора.
    5: (
        "Category 5 (gold): a story illustration with 1-2 expressive "
        "characters in a category-4-style fully realized realistic "
        "environment, showing one small ironic or humorous emotional "
        "narrative moment — a lighthearted, wry twist, not a purely "
        "neutral or dramatic scene. Characters remain the dominant focal "
        "point; no crowd or extras."
    ),
}


# ============================================================
# 4. CONCEPT-SPECIFIC VISUAL GUIDANCE
# ============================================================

def concept_visual_guidance(card) -> str:
    """
    Converts a loose concept into stronger visual instructions.

    This is deliberately generic enough to work with future cards,
    while adding special handling for the cinema example.

    IMPORTANT: avoid negation phrases here ("not like a coin", "no crowd
    of people" etc). CLIP-style text encoders used by diffusion models
    (including flux) do not reliably process negation — a phrase like
    "not a sphere" can actually reinforce the "sphere" concept instead of
    suppressing it. Anything that must be actively avoided belongs in
    concept_negative_terms() below, which feeds the real negative-prompt
    parameter, not natural-language negation in the positive prompt.
    """

    concept = card.concept.lower()

    if card.id == "prop_3dglasses_01":
        # См. ТЗ §0.10: катушка плёнки (щёки/зазор/намотка/отверстия) сама
        # по себе физически детализирована и конфликтует с правилом
        # категории 1 "объект без мелких деталей". 3D-очки — простой
        # силуэт (две линзы + дужка), без внутренней структуры, тема
        # читается сразу без текста. Катушка остаётся в коде как валидный
        # тест категории 3/4 (см. ветку "film reel" ниже), просто больше
        # не используется как тест категории 1.
        #
        # ИСПРАВЛЕНО: матчинг был по подстроке в card.concept ("3d glasses"
        # / "3d cinema glasses") — хрупко: любая переформулировка текста
        # концепта (см. ниже) грозила молча уронить всю ветку guidance до
        # generic-заглушки. Теперь матчим по card.id — стабильному
        # идентификатору, не зависящему от того, как именно сформулирован
        # текст.
        #
        # ДОБАВЛЕНО (3, после двух неудачных попыток): "glasses"/"eyewear"
        # грамматика уже поправлена (v7), явный "single e-commerce product
        # photo" + compel-вес уже добавлены (v10) — сетка всё равно
        # вылезала. Подозреваю, что дело не только в грамматике, а в самом
        # СУЩЕСТВИТЕЛЬНОМ: "glasses"/"eyewear" в обучающих данных сильно
        # ассоциируются с жанром "витрина оптики" (ряды очков на стенде) —
        # это самостоятельный композиционный паттерн, который тянет к себе
        # независимо от грамматического числа. "3D viewer" — реальный
        # исторический термин для ЭТОГО ЖЕ объекта (см. vintage-стерео-
        # коллекции 1920-х, где "cardboard viewing glasses" и "3D viewer"
        # взаимозаменяемы), грамматически единственное число, и не несёт
        # ассоциации "оптика/витрина".
        # ДОБАВЛЕНО (4, после seed-sweep на v12 — 6 разных seed, тот же
        # промпт): 5 из 6 всё ещё проваливались, но failure mode сменился —
        # больше не "человек в очках", а "существо/робот с лицом", у
        # которого линзы стали ГЛАЗАМИ. Гипотеза: два больших КРУГА,
        # соединённых перемычкой, сами по себе физически читаются как
        # "лицо" (глаза + переносица) независимо от текста — это
        # визуальный, а не текстовый bias. Меняю форму линз на
        # rounded-rectangle (как у реальных анаглифных очков) — менее
        # похоже на "круглые глаза", чем идеальный круг.
        return (
            "CONCEPT: (a single isolated vintage anaglyph 3D viewer, an "
            "e-commerce product photo, alone on a plain background, "
            "nothing else in the frame)1.4 — exactly one physical object, "
            "not a catalog page, not a grid, not a set, not a collection. "
            "A simple flat silhouette made of one left lens and one right "
            "lens joined by a small central bridge, with two folded temple "
            "arms — this is still ONE viewer, not two separate lenses "
            "floating apart. The lenses are soft rounded-rectangle shapes, "
            "like a rounded square — flat geometric panels, not circles, "
            "not round like eyes. One lens is a solid flat red/pink color, "
            "the other lens is a solid flat cyan/blue color. Plain smooth "
            "plastic frame, no visible screws, hinges, scratches, "
            "reflections, or surface texture. No mechanical parts, no fine "
            "internal structure — the whole object reads as a few large "
            "clean shapes. No printed labels or writing anywhere on it."
        )

    if "film reel" in concept or "movie reel" in concept:

        return (
            "CONCEPT: a vintage cinema film reel — a real physical object "
            "for storing motion-picture film. NOT a wheel, NOT a disc, "
            "NOT a coin, NOT a wheel hub. A thick central hub connects to "
            "TWO separate parallel circular metal flanges (plates) with a "
            "visible gap between them. A dense coil of dark film strip is "
            "physically wound in that gap between the two flanges, clearly "
            "visible around the circumference as a distinct darker material "
            "sandwiched between the gold metal flanges. One short loose end "
            "of film strip visibly exits the reel and curls outward. Each "
            "flange has 5 large triangular cut-out openings. Heavy solid "
            "metal construction with real physical thickness and depth "
            "between the two flanges — this is not flat. The reel floats "
            "weightlessly in empty air, completely alone, with nothing "
            "touching it and nothing visible beneath it. Three-quarter "
            "side view, tilted enough to show the gap and the wound film "
            "between the two flanges. No printed labels or writing "
            "anywhere on it."
        )

    if "movie director" in concept or "film director" in concept:
        # Наблюдение по логам: "A camera appears only as a small
        # supporting detail" звучало как НЕОБЯЗАТЕЛЬНЫЙ элемент — и модель
        # закономерно его дропала (то же QA FAIL раз за разом: "camera
        # missing entirely"). Переписано в explicit must-have: камера в
        # руках, а не "где-то у края кадра". Аналогично фон — было
        # достаточно абстрактно ("rich, atmospheric"), QA стабильно видел
        # "plain, abstract background" — усилено конкретными обязательными
        # деталями (rigs/cables/flats должны быть ВИДНЫ, не просто
        # подразумеваться атмосферой).
        #
        # ДОБАВЛЕНО: "vintage camera" без уточнения читается моделью (и
        # QA) как обычный фотоаппарат — QA прислал финальную реплику про
        # руки/хват именно фотографического хвата. Тема набора "кино", а
        # не "фотография" — камера должна быть кинокамерой.
        #
        # ДОБАВЛЕНО (2): вместо чисто описательного "vintage hand-crank
        # cinema movie camera" — используем узнаваемый термин категории/
        # эпохи "newsreel camera" / "crank camera". Это не бренд (в
        # отличие от, скажем, "Bolex" — реальный товарный знак, который
        # сознательно не используем в коммерческом ассете без отдельного
        # решения), но устойчивый термин из исторической/стоковой
        # фотографии с характерным узнаваемым силуэтом — даёт модели
        # более конкретный якорь, чем чисто описательный текст.
        return (
            "CONCEPT: a close-up / medium portrait shot, chest-up only — "
            "not a full-body shot, no visible feet, no wide shot with "
            "floor-standing equipment. An eccentric movie director, face "
            "and upper body filling most of the frame. The director is "
            "actively holding a vintage newsreel camera (a wooden-and-brass "
            "hand-crank film camera, like a 1920s silent-era newsreel crank "
            "camera) up near their chest, both hands gripping it — a boxy "
            "body with a distinctive side crank handle and a round flat "
            "film magazine/reel mounted on top, clearly NOT a modern photo "
            "camera and NOT a still-photography camera. The camera is a "
            "solid, clearly visible physical object, present in every "
            "single generation, not optional and not cropped out. "
            "BACKGROUND: the space directly behind the director must "
            "visibly show at least two of these concrete elements — "
            "hanging studio lighting rigs, thick cables coiled on the "
            "floor, canvas set flats, soundstage scaffolding — softly out "
            "of focus but clearly identifiable shapes, giving real spatial "
            "depth. Never a flat solid-color or smooth gradient backdrop "
            "with no visible objects in it. No other people, no readable "
            "text."
        )

    if card.type == "character":

        return (
            "CONCEPT: the character must be immediately recognizable from "
            "the supplied concept, with a strong readable silhouette, "
            "expressive face and clear pose performing the defining "
            "action described. No unnecessary extra characters, no text."
        )

    return (
        "CONCEPT: the object must unmistakably match the supplied concept "
        "— prioritize its recognizable silhouette and defining physical "
        "features over decorative detail. No text or typography."
    )


def concept_negative_terms(card) -> str:

    concept = card.concept.lower()

    if card.id == "prop_3dglasses_01":
        # Наблюдение по логам (и на zimage, и на local SDXL): "glasses"
        # у обеих моделей стабильно тянет за собой "кто-то их носит" —
        # то человек в очках, то пара стилизованных маскотов/роботов в
        # очках. Позитивный текст уже требует "no face" — этого мало,
        # усиливаем явным negative-блоком против лиц/персонажей/фигур,
        # а не только против конкретных типов очков.
        # ДОБАВЛЕНО: "glasses" грамматически множественное число — модель
        # раз за разом рисует ДВА объекта вместо одного (см. iteration 3:
        # буквальный "seamless pattern of multiple overlapping objects").
        # Явный запрет на паттерн/дубликаты/множественность.
        return (
            "sunglasses, goggles, ski goggles, VR headset, virtual reality "
            "headset, monocle, single lens, round wire-frame glasses, "
            "reading glasses, realistic photorealistic eyewear render, "
            "visible screws, visible hinge mechanism, scratches, dirt, "
            "worn texture, "
            "face, human face, cartoon face, character face, head, human head, "
            "person, human, people, man, woman, boy, girl, child, "
            "character, mascot, robot character, toy character, figure, "
            "portrait, bust, torso, body, hands, wearing glasses, "
            "someone wearing glasses, model wearing glasses, "
            "two characters, pair of characters, couple, "
            "two objects, multiple objects, several objects, duplicate, "
            "duplicated, repeated, repeating pattern, seamless pattern, "
            "tiled pattern, wallpaper pattern, collage, grid of objects, "
            "group of objects, set of objects, collection of objects, "
            "two pairs, second pair, another pair, "
            "(rows of objects, columns of objects, product catalog, "
            "catalog page, e-commerce grid, assortment, array of items, "
            "many pairs, dozens of objects, stock photo grid, texture "
            "pattern, fabric pattern, optician display, eyewear store "
            "display, sunglasses display rack, eyewear wall display, "
            "glasses shop shelf)1.3, "
            "(creature, blob creature, cute monster, plush toy, stuffed "
            "animal, googly eyes, eyeballs, cartoon creature, cartoon "
            "animal, alien creature, anthropomorphic, cute character face, "
            "creature face, robot face, toy face, face-like object, eyes "
            "looking at viewer, two eyes, pair of eyes)1.3"
        )

    if "film reel" in concept or "movie reel" in concept:
        # Круглая форма здесь корректна и ожидаема — в отличие от тикета,
        # запрещаем не "круглое", а "плоское/гладкое/безликое золото",
        # чтобы не потерять характерные щёки/намотку.
        # Анти-ракурсный негатив (front-facing/straight-on) убран: он не
        # чинит геометрию объекта, только конфликтовал с позитивным текстом
        # (одновременно просили "wheel-shaped" и "не колесо" — модель
        # закономерно застревала на колесе). Сначала чиним геометрию через
        # позитивное описание, negative для ракурса вернём, если понадобится.
        return (
            "plain smooth sphere, blank coin, unmarked medallion, "
            "featureless ball, generic jewelry, badge, shield, wheel, "
            "flat disc with no holes, solid disc, bowl shape, "
            "wagon wheel, cart wheel, bicycle wheel, gear, cog, pulley, "
            "mill wheel, spinning wheel, ground, floor, grass, field, "
            "dirt, lawn, meadow, stand, tripod stand, display stand, "
            "legs, base, pedestal, resting on a surface"
        )

    if "movie director" in concept or "film director" in concept:
        # QA неоднократно принимал камеру за оружие и деформировал
        # руки/камеру при попытке нарисовать взаимодействие с объективом —
        # подстраховка на случай, если модель всё ещё будет тянуть к этому.
        # Полный рост дублирован здесь же — модель упорно тянет к wide shot
        # с непропорционально огромной камерой на штативе.
        # Добавлено по логам: явный запрет на пустой/плоский фон и пустые
        # руки — ровно то, что QA видел раз за разом ("plain background",
        # "camera missing entirely"), а до этого в негативе не было вообще.
        # Добавлено: явный запрет на фотоаппарат — "vintage camera" без
        # уточнения модель по умолчанию тянет к обычной фотокамере
        # (QA финально жаловался именно на фотографический хват).
        return (
            "weapon, gun, pistol, blade, malformed hands, malformed camera equipment, "
            "full body shot, full length shot, wide shot, standing at a distance, "
            "visible feet, floor-standing tripod filling the frame, "
            "small distant figure, tiny character, "
            "plain background, solid color background, flat gradient background, "
            "empty background, minimalist background, blank backdrop, "
            "seamless paper backdrop, studio photography backdrop paper, "
            "empty hands, hands with nothing in them, hands at sides, "
            "arms crossed, no camera, missing camera, hidden camera, "
            "camera out of frame, portrait without props, "
            "photo camera, still camera, DSLR, photography camera, "
            "point-and-shoot camera, modern camera, smartphone, phone camera"
        )

    return ""


def category_negative_terms(category: int) -> str:
    """
    Категорийно-специфичный негатив (в отличие от concept_negative_terms —
    привязан к visual_category, а не к концепту). НЕЛЬЗЯ добавлять "shadow"
    в глобальный NEGATIVE_PROMPT — категории 2-5 требуют теней/земли, а вот
    категория 1 (floating) требует их полного отсутствия, и модель это
    правило игнорирует чаще всего — усиливаем реальным negative-параметром.
    """
    if category == 1:
        return (
            "cast shadow, drop shadow, ground shadow, contact shadow, "
            "floor, ground plane, table, surface, standing on a surface, "
            "reflection"
        )
    return ""


# ============================================================
# 5. DATA
# ============================================================

validated_cards = [
    {
        # ЗАМЕНА (ТЗ §0.10): prop_filmreel_01 -> prop_3dglasses_01.
        # Катушка плёнки физически детализирована (щёки/зазор/намотка/
        # отверстия) и конфликтует с правилом категории 1 "без мелких
        # деталей" — см. concept_visual_guidance(). 3D-очки дают тот же
        # тест (category 1, простой одиночный объект, тема "кино"), но с
        # силуэтом, который не требует внутренней детализации.
        "id": "prop_3dglasses_01",
        "set_id": "set_cinema",
        "type": "object",
        "rarity": "common",
        "visual_category": 1,
        "concept": (
            "a single vintage anaglyph 3D viewer, red-and-blue lenses — "
            "one physical object only, not two, not a set"
        ),
    },
    {
        "id": "char_director_01",
        "set_id": "set_cinema",
        "type": "character",
        "rarity": "gold",
        "visual_category": 5,
        "concept": (
            "a close-up chest-up portrait of an eccentric movie director's "
            "expressive face and shoulders, with only the corner of a "
            "small vintage newsreel crank camera barely visible at the "
            "edge of the frame"
        ),
    },
]


# ============================================================
# 6. CONFIGURATION
# ============================================================
# ВСЁ читается из env в момент импорта модуля. Если меняете значения в
# конфиг-ячейке ПОСЛЕ импорта в этой же kernel-сессии — перезапустите
# kernel или используйте importlib.reload(cp), иначе увидите старые
# значения (это уже обсуждалось при отладке VISION_API_KEY).

IMAGE_API_ENDPOINT = os.environ.get("IMAGE_API_ENDPOINT", "https://image.pollinations.ai/prompt/")
IMAGE_MODEL = os.environ.get("IMAGE_MODEL", "zimage")
IMAGE_WIDTH = int(os.environ.get("IMAGE_WIDTH", "1024"))
IMAGE_HEIGHT = int(os.environ.get("IMAGE_HEIGHT", "1024"))

# Раньше — жёстко захардкоженные относительные пути ("configs/...",
# "generated_assets"). Проблема (см. лог: resume "потерял" attempt-счётчик
# между сессиями): относительный путь резолвится от текущей рабочей
# директории kernel-процесса, которая не гарантированно совпадает между
# перезапусками Kaggle-сессии. Абсолютный путь под /kaggle/working — это
# стандартная примонтированная рабочая директория Kaggle-ноутбука, она
# переживает обычные перезапуски kernel В ПРЕДЕЛАХ той же интерактивной
# Edit-сессии.
#
# ЧЕСТНО: это НЕ решает персистентность МЕЖДУ полностью новыми сессиями
# (закрыл ноутбук и открыл через день/неделю без сохранённой версии) —
# для этого нужен либо "Save Version -> Save & Run All (with outputs)"
# (тогда файл попадёт в output закоммиченной версии и его можно скачать/
# подключить как input к следующей сессии), либо вынести
# EXPORT_JSON_PATH на подключённый Kaggle Dataset. Здесь просто убрана
# случайность "сработает/не сработает в зависимости от cwd" — стало явно
# и предсказуемо, а не автоматически вечно.
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/kaggle/working/generated_assets")
EXPORT_JSON_PATH = os.environ.get("EXPORT_JSON_PATH", "/kaggle/working/configs/cards_export_data.json")
# ДОБАВЛЕНО (v29): отдельная папка ТОЛЬКО для карт, реально прошедших QA
# (status=="Ready") — по запросу из диалога. OUTPUT_DIR копит вообще все
# попытки (Failed_QA, QA_Unavailable, промежуточные до финального PASS) —
# удобно для отладки, но неудобно для "найти готовое к сдаче". Файлы
# КОПИРУЮТСЯ (не переносятся) — оригиналы в OUTPUT_DIR остаются на месте,
# resume по ним продолжает работать как раньше.
READY_OUTPUT_DIR = os.environ.get("READY_OUTPUT_DIR", "/kaggle/working/final_cards")

# ИСПРАВЛЕНО: раньше были литералами (2 и 3), конфиг-ячейка их не влияла.
MAX_QA_RETRIES = int(os.environ.get("MAX_QA_RETRIES", "2"))
MAX_NETWORK_RETRIES = int(os.environ.get("MAX_NETWORK_RETRIES", "3"))

MAX_PARALLEL_CARDS = int(os.environ.get("MAX_PARALLEL_CARDS", "1"))

IMAGE_API_MIN_INTERVAL_SECONDS = float(os.environ.get("IMAGE_API_MIN_INTERVAL_SECONDS", "6.0"))

# ИСПРАВЛЕНО: раньше эти два таймаута задавались в конфиг-ячейке, но нигде
# не читались — запросы всегда шли с захардкоженным timeout=60.
IMAGE_API_TIMEOUT_SECONDS = float(os.environ.get("IMAGE_API_TIMEOUT_SECONDS", "60"))
VISION_API_TIMEOUT_SECONDS = float(os.environ.get("VISION_API_TIMEOUT_SECONDS", "60"))
# ДОБАВЛЕНО (v31): IMAGE_ENGINE="openrouter" — платный движок изображений
# через тот же OpenRouter-аккаунт (VISION_API_KEY/VISION_API_BASE_URL —
# см. §2.6/§3.4 в process_report: SDXL 1.0 base упирается в собственный
# потолок именно на Category 1 "floating isolated object", формулировка
# промпта тут ни при чём — тот же промпт в Gemini даёт чистый результат.
# Модель по умолчанию — google/gemini-2.5-flash-image ("Nano Banana"),
# ~$0.039/картинка (1024x1024) по цене на openrouter.ai на момент
# добавления — свериться перед долгим прогоном, как и с остальными
# моделями в этом файле.
IMAGE_GEN_MODEL = os.environ.get("IMAGE_GEN_MODEL", "google/gemini-2.5-flash-image")
IMAGE_GEN_MODEL_FALLBACKS = os.environ.get("IMAGE_GEN_MODEL_FALLBACKS", "openai/gpt-5-image-mini")
IMAGE_GEN_API_TIMEOUT_SECONDS = float(os.environ.get("IMAGE_GEN_API_TIMEOUT_SECONDS", "90"))
# ИСПРАВЛЕНО: без явного потолка модель заявляет свой дефолт (у
# некоторых платных — до 65536), и OpenRouter отказывает запросу
# HTTP 402 заранее, если баланс не покрывает этот ТЕОРЕТИЧЕСКИЙ
# максимум — даже когда реальный ответ стоит копейки. Дефолт здесь —
# подстраховка на случай, если конфиг-ячейка не выставила свою.
VISION_MAX_TOKENS = int(os.environ.get("VISION_MAX_TOKENS", "1500"))


# ============================================================
# 7. CARD DATACLASSES
# ============================================================

@dataclass
class Card:
    id: str
    set_id: str
    type: str
    rarity: str
    visual_category: int
    concept: str

    @staticmethod
    def from_dict(data: dict) -> "Card":
        required = ["id", "set_id", "type", "rarity", "visual_category", "concept"]
        missing = [f for f in required if f not in data]
        if missing:
            raise ValueError(f"Card data missing required fields: {missing}")

        category = data["visual_category"]
        if category not in CATEGORY_RULES:
            raise ValueError(f"Card '{data['id']}' has invalid category {category}")

        if category == 5 and data.get("rarity") != "gold":
            log.warning(
                "Card '%s': category 5 should normally be gold, but rarity='%s'",
                data["id"], data.get("rarity"),
            )

        return Card(**{f: data[f] for f in required})


@dataclass
class CardResult:
    card: Card
    status: str  # Ready | Failed_QA | Failed_Generation | QA_Unavailable
    asset_path: Optional[str] = None
    attempts: int = 0
    qa_history: list = field(default_factory=list)


# ============================================================
# 8. PROMPT BUILDER
# ============================================================

def build_prompt(card: Card, regeneration_hint: Optional[str] = None) -> str:
    category = card.visual_category

    parts = [
        f"MAIN SUBJECT: {card.concept}",
        f"STYLE: {MASTER_STYLE}, flat stylized cartoon illustration, "
        "not photorealistic, not a 3D render.",
        concept_visual_guidance(card),
        CATEGORY_RULES[category],
        "Strong single focal point, subject fully visible, no cropping. "
        "No text, letters, numbers, logos, or watermarks anywhere.",
    ]

    if regeneration_hint:
        parts.append(f"REQUIRED FIX (previous attempt failed QA): {regeneration_hint}")

    return "\n\n".join(parts)


# ============================================================
# 9. DETERMINISTIC SEED
# ============================================================

def deterministic_seed(card_id: str, attempt: int) -> int:
    digest = hashlib.sha256(f"{card_id}:{attempt}".encode("utf-8")).hexdigest()
    return int(digest, 16) % (2**31 - 1)


# ============================================================
# 9.1 PORTRAIT CROP WORKAROUND (ВРЕМЕННО ОТКЛЮЧЕНО)
# ============================================================
# Лог показал: crop честно применялся ("applied portrait crop"), но QA
# всё равно видел full-body — потому что модель не строит композицию под
# обрезку, она просто рисует человека в полный рост на любом холсте, а
# обрезка сверху забирает "полный рост с отрезанными ногами", а не
# "портрет по грудь". Это создавало ложное ощущение решённой проблемы в
# логах. Отключаю, пока не увидим, что позитивный промпт (теперь concept
# сам по себе описывает chest-up) действительно даёт нужную композицию —
# тогда обрезка станет осмысленной подстраховкой, а не костылём вместо
# исправления первопричины.

_PORTRAIT_CROP_ENABLED = False


def _portrait_crop_config(card) -> Optional[dict]:
    if not _PORTRAIT_CROP_ENABLED:
        return None
    concept = card.concept.lower()
    if "movie director" in concept or "film director" in concept:
        return {
            "gen_width": IMAGE_WIDTH,
            "gen_height": int(IMAGE_WIDTH * 1.4),  # генерируем выше квадрата
            "crop_to": (IMAGE_WIDTH, IMAGE_WIDTH),  # обрезаем до исходного квадрата
        }
    return None


def _apply_portrait_crop(filepath: str, crop_config: dict) -> None:
    from PIL import Image as PILImage

    crop_w, crop_h = crop_config["crop_to"]
    with PILImage.open(filepath) as img:
        # Берём верхнюю часть — там обычно лицо/плечи, а не ноги/штатив.
        left = 0
        top = 0
        right = min(crop_w, img.width)
        bottom = min(crop_h, img.height)
        cropped = img.crop((left, top, right, bottom))
        cropped.save(filepath)


# ============================================================
# 10. IMAGE API THROTTLING
# ============================================================

_image_api_lock = threading.Lock()
_image_api_last_call = 0.0


def _throttle_image_api():
    global _image_api_last_call
    with _image_api_lock:
        elapsed = time.monotonic() - _image_api_last_call
        wait = IMAGE_API_MIN_INTERVAL_SECONDS - elapsed
        if wait > 0:
            time.sleep(wait)
        _image_api_last_call = time.monotonic()


# ============================================================
# 10.1 KNOWN RATE-LIMIT PLACEHOLDER DETECTION
# ============================================================
# pollinations.ai иногда отвечает HTTP 200 с картинкой-заглушкой ВМЕСТО
# честного 429, когда сработал rate limit (задокументировано сообществом:
# https://github.com/pollinations/pollinations/issues/7207).
# Обычные изображения ~40-80 КБ, заглушка рейт-лимита заметно крупнее
# (по независимым наблюдениям ~1.3 МБ). Прежняя проверка "размер >
# 1024 байт" такую заглушку пропускала как валидный успех — отсюда
# изображения, вообще не связанные с промптом, при формальном 200 OK.

_KNOWN_RATE_LIMIT_PLACEHOLDER_MD5 = {
    "2090a5dc21c32952cbf8496339752bd1",
}
_SUSPICIOUSLY_LARGE_IMAGE_BYTES = 400_000  # обычный 1024x1024 результат в разы меньше


def _looks_like_rate_limit_placeholder(content: bytes) -> bool:
    digest = hashlib.md5(content).hexdigest()
    if digest in _KNOWN_RATE_LIMIT_PLACEHOLDER_MD5:
        return True
    # Хэш заглушки может со временем измениться на стороне pollinations —
    # аномальный размер ловит и варианты, которых нет в списке выше.
    return len(content) > _SUSPICIOUSLY_LARGE_IMAGE_BYTES


# ============================================================
# 11. IMAGE GENERATION
# ============================================================
# ТЗ-контекст: два движка за одним и тем же интерфейсом.
#   IMAGE_ENGINE=pollinations (по умолчанию) — как было, бесплатный
#       zimage через pollinations.ai. Ноль настройки, но: чужой чёрный
#       ящик, качество нестабильное, урезание длинного промпта под
#       вопросом (см. диагностику выше).
#   IMAGE_ENGINE=local — SDXL локально на GPU этого же Kaggle-ноутбука.
#       Тоже бесплатно (в рамках недельной GPU-квоты Kaggle), но seed,
#       negative_prompt и — САМОЕ ГЛАВНОЕ — длина промпта теперь под
#       нашим полным контролем, а не гадание "что там обрежет чужой сервер".
#
# Обе реализации отдают один и тот же bool-контракт, поэтому
# process_card/generate_preview/diagnose_prompt_ablation переключаются
# одной строкой конфига, без изменений в вызывающем коде.
#
# НАХОДКА (валидация архитектуры, 2026-08-20): prop_3dglasses_01 стабильно
# проваливал category-1 "single isolated object" на IMAGE_ENGINE=local
# (SDXL 1.0 base) — вместо одного предмета получали то персонажей, то
# сетку из десятков копий, то "существ" с лицом вместо линз (см. полную
# историю диагностики: MASTER_STYLE-фикс, "eyewear"->"3D viewer",
# compel-веса, форма линз, seed-sweep на 6 разных seed). Тот же самый
# итоговый промпт (see build_prompt output), поданный вручную в Gemini,
# дал чистый корректный результат С ПЕРВОЙ ПОДАЧИ. Вывод: build_prompt() /
# concept_visual_guidance() / concept_negative_terms() как система
# ВАЛИДНЫ — упирались в потолок конкретно голого SDXL 1.0 base без LoRA/
# файнтюна под product-shot, а не в качество промпт-инжиниринга. Решение
# зафиксировано: local (SDXL) остаётся дефолтным бесплатным движком как
# есть, дальше не дожимаем; при необходимости более высокого качества на
# сложных category-1 объектах — переключение IMAGE_ENGINE на платный
# провайдер (Gemini/GPT Image через OpenRouter, см. обсуждение раньше)
# архитектурно тривиально благодаря этому же диспетчеру, промпт менять
# не придётся.

IMAGE_ENGINE = os.environ.get("IMAGE_ENGINE", "pollinations").strip().lower()


def generate_image_api(
    prompt: str,
    negative_prompt: str,
    filepath: str,
    seed: int,
    max_retries: int = MAX_NETWORK_RETRIES,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> bool:
    if IMAGE_ENGINE == "local":
        return _generate_image_local(prompt, negative_prompt, filepath, seed, width=width, height=height)
    if IMAGE_ENGINE == "openrouter":
        return _generate_image_openrouter(
            prompt, negative_prompt, filepath, seed, max_retries=max_retries, width=width, height=height,
        )
    if IMAGE_ENGINE != "pollinations":
        log.warning("[ImageAPI] unknown IMAGE_ENGINE=%r, falling back to 'pollinations'", IMAGE_ENGINE)
    return _generate_image_pollinations(
        prompt, negative_prompt, filepath, seed, max_retries=max_retries, width=width, height=height,
    )


def _generate_image_pollinations(
    prompt: str,
    negative_prompt: str,
    filepath: str,
    seed: int,
    max_retries: int = MAX_NETWORK_RETRIES,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> bool:
    gen_width = width or IMAGE_WIDTH
    gen_height = height or IMAGE_HEIGHT

    # Снимаем compel-веса "(текст)1.4" — pollinations не умеет их
    # распарсить, скобки+число попали бы в промпт как обычный текст.
    prompt = _strip_compel_weights(prompt)
    negative_prompt = _strip_compel_weights(negative_prompt)

    # safe="" — критично: quote() по умолчанию не кодирует "/", а весь промпт
    # вставляется в ПУТЬ URL (не query), поэтому необработанный "/" в тексте
    # концепта (например "and/or") превращается в лишний сегмент пути и
    # ломает роутинг на сервере pollinations, давая 404 (воспроизведено:
    # категория 1 с "and/or" в concept_visual_guidance стабильно падала в 404,
    # категория 5 без "/" в тексте — нет).
    safe_prompt = quote(prompt, safe="")
    url = (
        f"{IMAGE_API_ENDPOINT}{safe_prompt}"
        f"?model={quote(IMAGE_MODEL)}"
        f"&width={gen_width}"
        f"&height={gen_height}"
        f"&seed={seed}"
        f"&nologo=true"
        f"&negative={quote(negative_prompt, safe='')}"
    )

    for attempt in range(max_retries):
        _throttle_image_api()
        try:
            # ИСПРАВЛЕНО: было захардкожено timeout=60
            response = _call_with_hard_timeout(
                requests.get, IMAGE_API_TIMEOUT_SECONDS + 10,
                url, timeout=IMAGE_API_TIMEOUT_SECONDS,
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else 5.0 * (attempt + 1)
                log.warning("[ImageAPI] 429 — waiting %.1fs", wait)
                if attempt < max_retries - 1:
                    time.sleep(wait)
                    continue
                return False

            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")
            if not content_type.startswith("image/"):
                raise ValueError(f"Image API returned non-image content: {content_type}")
            if len(response.content) < 1024:
                raise ValueError("Image response suspiciously small")
            if _looks_like_rate_limit_placeholder(response.content):
                # HTTP 200 + прошло по типу/размеру, но это заглушка
                # рейт-лимита, а не реальная генерация. Считаем это
                # транспортной ошибкой и ретраим — иначе мы бы приняли
                # изображение, вообще не связанное с промптом, за успех.
                raise ValueError(
                    f"Response looks like a rate-limit placeholder "
                    f"({len(response.content)} bytes) — not accepting as a real result"
                )

            with open(filepath, "wb") as f:
                f.write(response.content)

            log.info("[ImageAPI] image generated successfully")
            return True

        except (requests.exceptions.RequestException, ValueError) as e:
            log.warning("[ImageAPI] attempt %d/%d failed: %s", attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                time.sleep(2.0 ** attempt)

    return False


# ============================================================
# 10.2 OPENROUTER IMAGE GENERATION ENGINE (IMAGE_ENGINE="openrouter", ПЛАТНО)
# ============================================================
# ДОБАВЛЕНО (v31, см. §2.6/§3.4 process_report) — второй платный движок
# наравне с реализацией VisionQA/Orchestrator выше: тот же OpenRouter-
# аккаунт (VISION_API_KEY/VISION_API_BASE_URL), тот же принцип "primary +
# fallback через запятую", тот же bool-контракт, что и у остальных
# движков (generate_image_api переключает одной строкой конфига).
#
# OpenRouter пропускает генерацию изображений через ОБЫЧНЫЙ
# /chat/completions с "modalities": ["image"] (а не через отдельный
# images-эндпоинт для diffusion-моделей типа Stable Diffusion — у Gemini/
# GPT Image это мультимодальная генерация внутри chat completion).
# Картинка приходит в message.images[0].image_url.url как data URL
# ("data:image/png;base64,...") — не по ссылке, сразу base64.
#
# Негативный промпт у диффузионных моделей (SDXL) — отдельный канал
# генерации (CFG). У мультимодальных Gemini/GPT Image такого API-поля
# нет — они не diffusion-модели с classifier-free guidance, а
# инструкции понимают как обычный текст. Поэтому здесь negative_prompt
# просто дописывается в сам промпт как явное "Avoid:" — практический
# эквивалент для этого класса моделей.
#
# seed: OpenRouter не документирует гарантированную поддержку seed для
# image-модалити в chat/completions (в отличие от SDXL, где seed —
# честный параметр диффузии) — передаём его в payload на всякий случай
# (безвредно, если провайдер его просто проигнорирует), но НЕ полагаемся
# на детерминизм: в отличие от _generate_image_local, здесь разные
# попытки и так гарантированно дадут разные картинки за счёт обычной
# вариативности мультимодальной генерации, а не за счёт seed.

def _generate_image_openrouter(
    prompt: str,
    negative_prompt: str,
    filepath: str,
    seed: int,
    max_retries: int = MAX_NETWORK_RETRIES,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> bool:
    base_url = os.environ.get("VISION_API_BASE_URL", "")
    api_key = os.environ.get("VISION_API_KEY", "")
    if not (base_url and api_key):
        log.error(
            "[ImageGen] IMAGE_ENGINE=openrouter requires VISION_API_BASE_URL/"
            "VISION_API_KEY to be set (same OpenRouter account as Vision QA)."
        )
        return False

    models = [m.strip() for m in
              ([IMAGE_GEN_MODEL] + IMAGE_GEN_MODEL_FALLBACKS.split(","))
              if m.strip()]

    gen_width = width or IMAGE_WIDTH
    gen_height = height or IMAGE_HEIGHT
    import math
    g = math.gcd(gen_width, gen_height) or 1
    aspect_ratio = f"{gen_width // g}:{gen_height // g}"

    full_prompt = prompt
    if negative_prompt:
        full_prompt += f"\n\nAvoid entirely: {negative_prompt}."

    last_error = None

    for attempt in range(max_retries):
        for model_index, model in enumerate(models):
            payload = {
                "model": model,
                "messages": [{"role": "user", "content": full_prompt}],
                "modalities": ["image"],
                "seed": seed,
                "image_config": {"aspect_ratio": aspect_ratio},
            }

            try:
                _throttle_image_api()
                log.info("[ImageGen] model '%s' — generating (attempt %d/%d)...",
                          model, attempt + 1, max_retries)

                response = _call_with_hard_timeout(
                    requests.post, IMAGE_GEN_API_TIMEOUT_SECONDS + 10,
                    f"{base_url.rstrip('/')}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                    timeout=IMAGE_GEN_API_TIMEOUT_SECONDS,
                )

                if response.status_code == 429 or 400 <= response.status_code < 500:
                    raise ValueError(f"HTTP {response.status_code} — {response.text[:300]}")
                response.raise_for_status()

                data = response.json()
                try:
                    images = data["choices"][0]["message"].get("images") or []
                except (KeyError, IndexError, TypeError) as e:
                    raise ValueError(
                        f"Unexpected response shape ({e}) — raw: {str(data)[:500]}"
                    )
                if not images:
                    raise ValueError(f"No images in response — raw: {str(data)[:500]}")

                image_url = images[0]["image_url"]["url"]
                if not image_url.startswith("data:"):
                    raise ValueError(f"Expected a data: URL, got: {image_url[:80]}")
                b64_data = image_url.split(",", 1)[1]
                image_bytes = base64.b64decode(b64_data)
                if len(image_bytes) < 1024:
                    raise ValueError(f"Decoded image suspiciously small ({len(image_bytes)} bytes)")

                with open(filepath, "wb") as f:
                    f.write(image_bytes)

                log.info("[ImageGen] model '%s' — image generated successfully (seed=%d)", model, seed)
                return True

            except (requests.exceptions.RequestException, ValueError, KeyError, IndexError) as e:
                last_error = e
                log.warning("[ImageGen] model '%s' attempt %d/%d failed: %s",
                            model, attempt + 1, max_retries, e)
                if model_index < len(models) - 1:
                    log.info("[ImageGen] switching to fallback '%s'", models[model_index + 1])

        if attempt < max_retries - 1:
            time.sleep(2.0 ** attempt)

    log.error("[ImageGen] all models/attempts exhausted: %s. Last error: %s", models, last_error)
    return False


# ============================================================
# 11.1 LOCAL DIFFUSION ENGINE (SDXL ON KAGGLE GPU, FREE)
# ============================================================
# Загружаем модель ОДИН раз (глобальный синглтон) — вес ~7 ГБ, грузить
# его на каждый вызов process_card убьёт всю экономию времени. Первый
# вызов в ноутбуке будет заметно медленнее (скачивание + загрузка на GPU),
# дальше — только сам инференс.
#
# ВАЖНО (то, из-за чего мы вообще сюда пришли — см. диагностику длины
# промпта): у SDXL текстовый энкодер CLIP жёстко режет на 77 токенах.
# Наш полный build_prompt() — это ~1200-1600 символов, то есть далеко за
# 77 токенами. Без доп. библиотеки CLIP молча обрежет промпт первыми
# ~60-70 словами и отбросит всё остальное — то есть локально мы рискуем
# наступить на ТУ ЖЕ грабли с обрезкой, только теперь она документирована
# и предсказуема, а не "чёрный ящик pollinations". Решаем это через
# `compel` — стандартную библиотеку для chunked-промптов у SDXL, которая
# объединяет эмбеддинги по кускам вместо обрезки.

LOCAL_IMAGE_MODEL = os.environ.get("LOCAL_IMAGE_MODEL", "stabilityai/stable-diffusion-xl-base-1.0")
LOCAL_IMAGE_STEPS = int(os.environ.get("LOCAL_IMAGE_STEPS", "30"))
LOCAL_IMAGE_GUIDANCE_SCALE = float(os.environ.get("LOCAL_IMAGE_GUIDANCE_SCALE", "7.0"))

_local_pipeline = None
_local_compel = None
_local_pipeline_lock = threading.Lock()


def reset_local_diffusion_pipeline() -> None:
    """
    Принудительно выгружает синглтон SDXL-пайплайна из GPU-памяти и чистит
    CUDA-аллокатор — без перезапуска kernel'а.

    Когда это нужно: если после CUDA OOM карта остаётся забитой почти под
    завязку ещё ДО загрузки новой модели (см. лог — "14.54 GiB in use"
    перед стартом генерации). empty_cache() внутри except-блока чистит
    только "reserved but unallocated" память — объекты, которые Python (в
    Jupyter особенно) всё ещё где-то держит живыми через traceback упавшего
    исключения, этим не освобождаются. del + gc.collect() + empty_cache()
    — первая попытка перед тем, как идти на полный Restart Kernel.

    ВАЖНО (честно): это не гарантированно решает проблему — если утечка
    держится через ссылку в traceback интерактивной Jupyter-сессии за
    пределами этого модуля (например, вывод предыдущей ошибочной ячейки),
    единственный надёжный способ — Kernel -> Restart, затем Run All заново.
    """
    global _local_pipeline, _local_compel
    with _local_pipeline_lock:
        if _local_pipeline is None:
            log.info("[LocalDiffusion] nothing to reset — pipeline was not loaded")
            return
        del _local_pipeline
        del _local_compel
        _local_pipeline = None
        _local_compel = None

    import gc
    gc.collect()
    try:
        import torch
        torch.cuda.empty_cache()
        torch.cuda.ipc_collect()
        free, total = torch.cuda.mem_get_info()
        log.info(
            "[LocalDiffusion] pipeline unloaded, cache cleared — GPU free: %.2f/%.2f GiB",
            free / 1e9, total / 1e9,
        )
    except Exception as e:
        log.warning("[LocalDiffusion] cleanup ran but couldn't query GPU memory: %s", e)


class LocalDiffusionNotAvailable(RuntimeError):
    pass


def _get_local_pipeline():
    """
    Ленивая инициализация SDXL-пайплайна + compel (для промптов >77
    токенов). Потокобезопасно (double-checked locking) — на случай если
    когда-нибудь MAX_PARALLEL_CARDS > 1 для локального движка.
    """
    global _local_pipeline, _local_compel
    if _local_pipeline is not None:
        return _local_pipeline, _local_compel

    with _local_pipeline_lock:
        if _local_pipeline is not None:
            return _local_pipeline, _local_compel

        try:
            import torch
            from diffusers import StableDiffusionXLPipeline, AutoencoderKL
            from compel import Compel, ReturnedEmbeddingsType
        except ImportError as e:
            raise LocalDiffusionNotAvailable(
                f"Missing dependency for IMAGE_ENGINE=local: {e}. "
                f"Run: pip install diffusers accelerate safetensors compel --quiet"
            ) from e

        if not torch.cuda.is_available():
            raise LocalDiffusionNotAvailable(
                "IMAGE_ENGINE=local requires a CUDA GPU, but torch.cuda.is_available() "
                "is False. In Kaggle: Notebook Settings -> Accelerator -> GPU T4 x2 "
                "(or P100), then restart the session."
            )

        # ИСПРАВЛЕНО (лог: CUDA OOM на T4 16GB + следом "Input type (c10::Half)
        # and bias type (float) should be the same" на другой карте той же
        # сессии). Причина обеих ошибок одна: у SDXL "родной" fp16-VAE
        # переполняется (NaN) в чистом float16, поэтому diffusers по
        # умолчанию МОЛЧА апкастит VAE в float32 прямо перед decode-шагом
        # (тот самый deprecation warning "upcast_vae" в логе) — это и
        # временный скачок VRAM (fp32-VAE вдвое тяжелее), и, судя по всему,
        # именно этот скрытый апкаст спровоцировал рассинхрон dtype после
        # того как первый вызов уже упал по памяти в середине операции.
        # Стандартное комьюнити-решение: madebyollin/sdxl-vae-fp16-fix —
        # VAE, переобученный так, чтобы не переполняться в float16 —
        # апкаст больше не нужен вообще, снимает разом обе проблемы.
        #
        # try/except вокруг ЗАГРУЗКИ (не только генерации) — по логу видно,
        # что OOM может случиться уже на этапе .to("cuda"), и без явной
        # очистки здесь частично загруженные vae/pipe остаются висеть в
        # памяти (в Jupyter — через traceback предыдущей ошибки) до
        # следующего падения, пока не съедят всю карту целиком.
        try:
            log.info("[LocalDiffusion] loading fp16-safe VAE (madebyollin/sdxl-vae-fp16-fix)...")
            vae = AutoencoderKL.from_pretrained(
                "madebyollin/sdxl-vae-fp16-fix", torch_dtype=torch.float16,
            )

            log.info("[LocalDiffusion] loading '%s' onto GPU (first call only, may take a few minutes)...", LOCAL_IMAGE_MODEL)
            pipe = StableDiffusionXLPipeline.from_pretrained(
                LOCAL_IMAGE_MODEL,
                vae=vae,
                torch_dtype=torch.float16,
                variant="fp16",
                use_safetensors=True,
            )
            pipe = pipe.to("cuda")
            pipe.set_progress_bar_config(disable=True)
            try:
                pipe.enable_xformers_memory_efficient_attention()
            except Exception:
                # Опционально — если xformers не установлен/не собрался,
                # компенсируем attention/vae slicing ниже (дешевле по VRAM
                # ценой небольшой просадки скорости — на T4 16GB это разумный
                # размен, особенно после того как один прогон уже упёрся в OOM).
                log.info("[LocalDiffusion] xformers unavailable, using attention/VAE slicing instead")
            pipe.enable_attention_slicing()
            # ИСПРАВЛЕНО: pipe.enable_vae_slicing() — deprecated (видели
            # FutureWarning в каждом логе), актуальный вызов — на самом
            # pipe.vae. Заодно включаем vae tiling — доп. подстраховка по
            # памяти на 1024x1024 (рекомендация из ревью).
            pipe.vae.enable_slicing()
            pipe.vae.enable_tiling()

            compel = Compel(
                tokenizer=[pipe.tokenizer, pipe.tokenizer_2],
                text_encoder=[pipe.text_encoder, pipe.text_encoder_2],
                returned_embeddings_type=ReturnedEmbeddingsType.PENULTIMATE_HIDDEN_STATES_NON_NORMALIZED,
                requires_pooled=[False, True],
            )

        except Exception:
            # Загрузка упала на полпути (например, OOM на .to("cuda")) —
            # gc.collect() + empty_cache() до re-raise, чтобы частично
            # загруженные объекты не висели в памяти до следующего падения
            # (см. docstring reset_local_diffusion_pipeline про
            # traceback-удержание в Jupyter). Явный del по имени переменной
            # здесь не сработал бы — locals() внутри функции в CPython это
            # снимок, а не живой namespace, поэтому просто выходим из
            # scope через raise и полагаемся на обычную очистку кадра.
            import gc
            gc.collect()
            torch.cuda.empty_cache()
            raise

        _local_pipeline = pipe
        _local_compel = compel
        log.info("[LocalDiffusion] model loaded and ready")
        return _local_pipeline, _local_compel


def _generate_image_local(
    prompt: str,
    negative_prompt: str,
    filepath: str,
    seed: int,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> bool:
    gen_width = width or IMAGE_WIDTH
    gen_height = height or IMAGE_HEIGHT

    try:
        import torch
        pipe, compel = _get_local_pipeline()

        # НАЙДЕНО ПРИ РЕВЬЮ (gemini): _local_pipeline_lock раньше защищал
        # только ЗАГРУЗКУ пайплайна в _get_local_pipeline() — сам вызов
        # инференса (compel(...) + pipe(...)) ничем не был защищён. Пока
        # MAX_PARALLEL_CARDS=1 (текущий конфиг) это не стреляло — но если
        # кто-то поднимет параллелизм на более быстрой GPU, два потока
        # одновременно отправят тензоры в один и тот же CUDA-контекст,
        # что гарантированно уронит PyTorch или даст OOM. Дешёвая
        # защита — берём тот же лок на время самого инференса.
        with _local_pipeline_lock:
            # compel строит эмбеддинги кусками по 77 токенов и склеивает их —
            # длина промпта (в отличие от "сырого" pipe(prompt=...)) больше не
            # режется молча. pad_conditioning_tensors_to_same_length нужен,
            # т.к. positive и negative могут разбиться на разное число кусков.
            conditioning, pooled = compel(prompt)
            negative_conditioning, negative_pooled = compel(negative_prompt)
            [conditioning, negative_conditioning] = compel.pad_conditioning_tensors_to_same_length(
                [conditioning, negative_conditioning]
            )

            generator = torch.Generator(device="cuda").manual_seed(seed)

            result = pipe(
                prompt_embeds=conditioning,
                pooled_prompt_embeds=pooled,
                negative_prompt_embeds=negative_conditioning,
                negative_pooled_prompt_embeds=negative_pooled,
                width=gen_width,
                height=gen_height,
                num_inference_steps=LOCAL_IMAGE_STEPS,
                guidance_scale=LOCAL_IMAGE_GUIDANCE_SCALE,
                generator=generator,
            )
            image = result.images[0]
        image.save(filepath)
        log.info(
            "[LocalDiffusion] image generated successfully (seed=%d, steps=%d, %dx%d)",
            seed, LOCAL_IMAGE_STEPS, gen_width, gen_height,
        )
        return True

    except LocalDiffusionNotAvailable as e:
        log.error("[LocalDiffusion] %s", e)
        return False
    except Exception as e:
        log.error("[LocalDiffusion] generation failed: %s", e)
        try:
            import gc
            import torch
            gc.collect()
            torch.cuda.empty_cache()
        except Exception:
            pass
        return False


# ============================================================
# 12. VISION CONFIG
# ============================================================

def _get_vision_config():
    base_url = os.environ.get("VISION_API_BASE_URL", "")
    api_key = os.environ.get("VISION_API_KEY", "")
    primary = os.environ.get("VISION_MODEL", "")
    fallbacks_raw = os.environ.get("VISION_MODEL_FALLBACKS", "")
    fallbacks = [m.strip() for m in fallbacks_raw.split(",") if m.strip()]

    models = []
    if primary:
        models.append(primary)
    for m in fallbacks:
        if m not in models:
            models.append(m)

    return base_url, api_key, models


# ============================================================
# 13. VISION THROTTLING
# ============================================================

VISION_API_MIN_INTERVAL_SECONDS = float(os.environ.get("VISION_API_MIN_INTERVAL_SECONDS", "4.0"))
MAX_VISION_RETRIES = int(os.environ.get("MAX_VISION_RETRIES", "1"))

_vision_api_lock = threading.Lock()
_vision_api_last_call = 0.0


def _throttle_vision_api():
    global _vision_api_last_call
    with _vision_api_lock:
        elapsed = time.monotonic() - _vision_api_last_call
        wait = VISION_API_MIN_INTERVAL_SECONDS - elapsed
        if wait > 0:
            time.sleep(wait)
        _vision_api_last_call = time.monotonic()


# ============================================================
# 14. VISION QA SYSTEM PROMPT
# ============================================================

QA_SYSTEM_PROMPT = """
You are a strict visual QA inspector for premium casual mobile game art.

Inspect the attached image and compare it with the supplied concept and category.

Evaluate exactly five criteria:

1. CATEGORY_COMPLIANCE
Does the image obey the assigned visual category?

2. SUBJECT_CLARITY
Is the requested subject clearly recognizable, fully visible and uncropped?

3. ARTIFACT_CHECK
Are there forbidden artifacts such as text, letters, numbers, logos,
watermarks, pseudo-writing, gibberish, blur, severe noise, malformed anatomy,
duplicate objects or unnecessary characters?

Exception: a single individual letter, digit, or symbol printed on ONE
key/button/dial of a functional object whose normal real-world design
requires per-key markings (e.g. one typewriter key showing "A", one
piano key, a rotary dial digit, a gauge numeral) is NOT a forbidden
artifact by itself — this is an unavoidable part of that object's basic
recognizable form, not decorative text. This exception does NOT cover:
any readable word, brand name, title, logo, sign, or label; garbled
pseudo-word gibberish (e.g. nonsense strings that read as fake words);
or multiple keys together spelling something legible. If in doubt
whether a marking reads as an actual word/logo rather than an isolated
per-key character, treat it as a forbidden artifact.

4. STYLE_CONSISTENCY
Does it look like polished premium casual mobile game collectible card art?

5. CONCEPT_MATCH
Is the depicted subject actually the requested concept,
rather than a generic or unrelated object?

Important:
A concept mismatch is NOT an artifact.
Classify the failure under CONCEPT_MATCH.

If the image passes all five criteria, return PASS.

If any criterion fails, return FAIL and give one concise actionable fix.

Return STRICT JSON only:

{
  "status": "PASS" or "FAIL",
  "failed_criteria": ["CATEGORY_COMPLIANCE", "..."],
  "reasoning": "brief explanation",
  "regeneration_instructions": "specific fix"
}
""".strip()


# ============================================================
# 15. VISION EXCEPTIONS
# ============================================================

class VisionQANotConfigured(RuntimeError):
    pass


class VisionQAUnavailable(RuntimeError):
    pass


# ============================================================
# 16. IMAGE BASE64
# ============================================================

def _encode_image_b64(filepath: str) -> str:
    with open(filepath, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


# ============================================================
# 17. VISION REQUEST
# ============================================================

def _post_vision_request(base_url: str, api_key: str, payload: dict) -> requests.Response:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    url = f"{base_url.rstrip('/')}/chat/completions"

    last_error = None
    for attempt in range(MAX_VISION_RETRIES):
        _throttle_vision_api()

        wait_start = time.monotonic()
        log.info("[VisionQA] model '%s' — sending request (timeout=%.0fs)...",
                  payload.get("model"), VISION_API_TIMEOUT_SECONDS)
        try:
            # ИСПРАВЛЕНО: было захардкожено timeout=60
            response = _call_with_hard_timeout(
                requests.post, VISION_API_TIMEOUT_SECONDS + 10,
                url, headers=headers, json=payload, timeout=VISION_API_TIMEOUT_SECONDS,
            )
            elapsed = time.monotonic() - wait_start
            log.info("[VisionQA] model '%s' responded in %.1fs (HTTP %d)",
                      payload.get("model"), elapsed, response.status_code)

            # 429 не ретраим здесь — free-модели часто rate-limited upstream,
            # ожидание тратит время впустую, вызывающий код переключится
            # на fallback-модель.
            if response.status_code == 429:
                body = response.text[:500]
                raise VisionQAUnavailable(
                    f"429 rate limited for model '{payload.get('model')}' — {body}"
                )

            if 400 <= response.status_code < 500:
                body = response.text[:500]
                raise VisionQAUnavailable(
                    f"{response.status_code} client error for model '{payload.get('model')}' — {body}"
                )

            response.raise_for_status()
            return response

        except VisionQAUnavailable:
            raise

        except requests.exceptions.RequestException as e:
            elapsed = time.monotonic() - wait_start
            last_error = e
            log.warning("[VisionQA] model '%s' network error after %.1fs: %s",
                        payload.get("model"), elapsed, e)
            if attempt < MAX_VISION_RETRIES - 1:
                time.sleep(2.0 * (attempt + 1))

    raise VisionQAUnavailable(f"Vision request failed for model '{payload.get('model')}': {last_error}")


# ============================================================
# 18. JSON CLEANUP
# ============================================================

def _atomic_json_write(path: str, data: dict) -> None:
    """
    Пишет JSON атомарно: во временный файл в той же директории, затем
    os.replace() в целевой путь. os.replace() атомарен на уровне ФС
    (POSIX) — читатель либо видит старый файл целиком, либо новый
    целиком, никогда не усечённый огрызок от прерванной на середине
    записи (сбой kernel'а, OOM-kill, обрыв сессии Kaggle). Раньше писали
    напрямую в целевой путь — при падении ровно в момент записи
    EXPORT_JSON_PATH/COLLECTION_EXPORT_JSON_PATH стали бы битыми, и
    resume на следующем прогоне сломался бы вместо того, чтобы
    продолжить с последнего целого состояния.
    """
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=directory or ".", prefix=os.path.basename(path) + ".", suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


def _log_quota_reset_if_present(error_text: str, context: str) -> None:
    """
    Достаёт X-RateLimit-Reset (epoch ms) из сырого текста ошибки
    OpenRouter, если он там есть, и логирует человекочитаемое время
    сброса. Без этого приходится вручную конвертировать epoch-таймстамп
    каждый раз, когда упирается в free-models-per-day (см. историю этого
    диалога — делал это вручную минимум трижды).
    """
    match = re.search(r'"X-RateLimit-Reset"\s*:\s*"?(\d{10,13})"?', error_text)
    if not match:
        return
    try:
        import datetime
        reset_ms = int(match.group(1))
        reset_dt = datetime.datetime.fromtimestamp(reset_ms / 1000, tz=datetime.timezone.utc)
        log.error("[%s] daily free-tier quota resets at %s UTC", context, reset_dt.isoformat())
    except (ValueError, OSError):
        pass


def _clean_json_response(raw_text: str) -> str:
    # ИСПРАВЛЕНО (ревью): раньше startswith("```json") ловил только точный
    # случай, когда ответ модели НАЧИНАЕТСЯ с ограждения. Свободные модели
    # нередко добавляют вступление ("Here is the evaluation result:
    # ```json ...") — на таком тексте startswith молча не срабатывал, и
    # json.loads() падал на всём тексте целиком, включая вступление.
    # Теперь ищем JSON-блок в ограждении ГДЕ УГОДНО в тексте; если
    # ограждения нет вообще — берём внешний диапазон от первой "{" до
    # последней "}" (страховка на случай, если модель просто не
    # обернула JSON в ```).
    text = raw_text.strip()

    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if fenced:
        return fenced.group(1).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start:end + 1].strip()

    return text


# ============================================================
# 19. REAL VISION QA
# ============================================================

def perform_image_qa(filepath: str, card: Card):
    base_url, api_key, models = _get_vision_config()

    if not (base_url and api_key and models):
        raise VisionQANotConfigured("Vision QA is not configured.")

    image_b64 = _encode_image_b64(filepath)

    user_prompt = f"""
CONCEPT:
{card.concept}

ASSIGNED CATEGORY:
{card.visual_category} — {CATEGORY_LABELS[card.visual_category]}

CATEGORY RULE:
{CATEGORY_RULES[card.visual_category]}

CONCEPT VISUAL REQUIREMENT:
{concept_visual_guidance(card)}

Check the attached image.
""".strip()

    # ИСПРАВЛЕНО (по факту из реального лога прогона на 160 картах):
    # весь fallback-список (primary + все fallbacks) исчерпывался за
    # секунды, и падал в QA_Unavailable, даже когда причиной был не
    # дневной лимит, а КРАТКОВРЕМЕННЫЙ затор на одном апстрим-провайдере
    # (`upstream_provider_shared_pool` 429 у обоих google/gemma-* сразу —
    # это ожидаемо: оба fallback'а сидят на одном и том же провайдере,
    # см. комментарий в config_cell про VISION_MODEL_FALLBACKS). На
    # практике этот же провайдер отвечал нормально уже через несколько
    # карт — то есть один короткий повтор ВСЕЙ цепочки после паузы
    # часто спасает попытку, которую иначе спишет circuit breaker.
    # Диверсифицировать fallback на третьего провайдера сейчас НЕЛЬЗЯ
    # честно: на момент правки единственные подтверждённо бесплатные
    # vision-модели на OpenRouter — это ровно nemotron-omni и два
    # gemma (см. проверку в процессе ревью) — третьего живого
    # бесплатного vision-провайдера просто не существует прямо сейчас,
    # это не упущение конфига.
    rounds = max(1, int(os.environ.get("VISION_UNAVAILABLE_RETRY_ROUNDS", "2")))
    backoff_seconds = float(os.environ.get("VISION_UNAVAILABLE_BACKOFF_SECONDS", "20.0"))

    last_error = None

    for round_index in range(rounds):
        for model_index, model in enumerate(models):
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": QA_SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": user_prompt},
                            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}},
                        ],
                    },
                ],
                "temperature": 0,
                "max_tokens": VISION_MAX_TOKENS,
            }

            try:
                response = _post_vision_request(base_url, api_key, payload)
                raw_text = response.json()["choices"][0]["message"]["content"]
                clean = _clean_json_response(raw_text)
                parsed = json.loads(clean)

                status = parsed.get("status") == "PASS"
                failed = parsed.get("failed_criteria", [])
                reasoning = parsed.get("reasoning", "")
                fix = parsed.get("regeneration_instructions")

                log.info("[VisionQA] model '%s' result: %s", model, "PASS" if status else "FAIL")
                return status, failed, reasoning, fix, model

            except VisionQAUnavailable as e:
                last_error = e
                if model_index < len(models) - 1:
                    next_model = models[model_index + 1]
                    log.info("[VisionQA] switching to fallback '%s'", next_model)
                    continue
                break

            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
                last_error = e
                # ИСПРАВЛЕНО: раньше здесь терялось тело ответа — в логе
                # был виден только текст исключения ("'choices'"), а не
                # то, что модель реально прислала (часто это скрытый
                # error-объект под HTTP 200, а не пустой ответ).
                body_snippet = ""
                try:
                    body_snippet = f" — raw body: {response.text[:300]!r}"
                except NameError:
                    pass
                log.warning("[VisionQA] invalid response from model '%s': %s%s", model, e, body_snippet)
                if model_index < len(models) - 1:
                    next_model = models[model_index + 1]
                    log.info("[VisionQA] switching to fallback '%s'", next_model)
                    continue
                break

            except requests.exceptions.RequestException as e:
                last_error = e
                log.warning("[VisionQA] model '%s' request error: %s", model, e)
                if model_index < len(models) - 1:
                    next_model = models[model_index + 1]
                    log.info("[VisionQA] switching to fallback '%s'", next_model)
                    continue
                break

        if round_index < rounds - 1:
            log.warning(
                "[VisionQA] all %d model(s) unavailable this round — waiting %.0fs before "
                "one retry of the full chain (transient shared-pool congestion, not "
                "necessarily daily quota) — last error: %s",
                len(models), backoff_seconds, last_error,
            )
            time.sleep(backoff_seconds)

    _log_quota_reset_if_present(str(last_error), "VisionQA")
    raise VisionQAUnavailable(f"All Vision models unavailable: {models}. Last error: {last_error}")


# ============================================================
# 20. PROCESS ONE CARD
# ============================================================

def _display_inline(filepath: str, card_id: str, attempt: int) -> None:
    """
    Показывает картинку прямо в выводе ячейки сразу после генерации —
    до QA, чтобы изображение было видно, даже если QA потом упадёт,
    зависнет или пайплайн прервётся на следующем шаге. Тихо не делает
    ничего вне Jupyter/IPython (например, при запуске как обычный
    .py-скрипт).
    """
    try:
        from IPython.display import Image as IPyImage, display
        print(f"[{card_id}] iteration {attempt} — сгенерированное изображение:")
        display(IPyImage(filename=filepath, width=320))
    except ImportError:
        pass
    except Exception as e:
        log.warning("[%s] inline preview failed (non-fatal): %s", card_id, e)


def process_card(card: Card, attempt_offset: int = 0) -> CardResult:
    """
    attempt_offset: сколько попыток генерации по этой карте УЖЕ было
    сделано в предыдущих прогонах пайплайна (берётся из EXPORT_JSON_PATH
    при resume). Нужен, чтобы seed НЕ повторялся до бесконечности.

    Раньше: deterministic_seed(card.id, attempt) зависел только от
    номера итерации внутри ТЕКУЩЕГО прогона (1, 2, 3) — а значит при
    каждом перезапуске пайплайна карта получала ровно те же 3 seed'а,
    что и в прошлый раз. Если все три оказались неудачными — это
    гарантированный вечный цикл: 3 фиксированных броска кубика, и точка.

    Теперь: реальный номер попытки = attempt_offset + attempt (счётчик
    сквозной, растёт от прогона к прогону). Seed остаётся ДЕТЕРМИНИРО-
    ВАННЫМ (воспроизводим по номеру попытки — можно найти, каким seed'ом
    был сгенерирован конкретный ассет), но каждый новый прогон исследует
    новую часть пространства seed, а не пересдаёт те же карты.
    """
    log.info("[%s] Category %d (%s)", card.id, card.visual_category, CATEGORY_LABELS[card.visual_category])
    if attempt_offset:
        log.info(
            "[%s] resume: %d prior attempt(s) recorded — continuing seed "
            "sequence from attempt #%d (not repeating past seeds)",
            card.id, attempt_offset, attempt_offset + 1,
        )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # ДОБАВЛЕНО: раньше имя файла было просто "{card.id}.png" — глядя на
    # папку с картинками, нельзя было понять ни категорию, ни редкость,
    # ни (до открытия JSON) прошла ли карта QA. Категория/редкость
    # известны сразу (не меняются по ходу попыток) — зашиваем их в
    # базовое имя. Статус (READY/FAILED_QA/...) известен только ПОСЛЕ
    # цикла попыток — дописывается переименованием в конце функции.
    base_filename = f"{card.id}_cat{card.visual_category}_{card.rarity}"
    filepath = os.path.join(OUTPUT_DIR, base_filename + ".png")

    # Подчищаем файлы с чужим статусом от ПРОШЛОГО прогона той же карты
    # (например, "..._FAILED_QA.png" перед повторной попыткой через
    # resume) — иначе на диске накопится несколько файлов на одну карту
    # с разными устаревшими статусами.
    for stale in glob.glob(os.path.join(OUTPUT_DIR, base_filename + "_*.png")):
        try:
            os.remove(stale)
        except OSError:
            pass

    result = CardResult(card=card, status="Failed_Generation")
    regeneration_hint = None
    total_attempts = MAX_QA_RETRIES + 1

    for attempt in range(1, total_attempts + 1):
        global_attempt = attempt_offset + attempt
        result.attempts = global_attempt
        log.info("[%s] iteration %d/%d (global attempt #%d)", card.id, attempt, total_attempts, global_attempt)

        prompt = build_prompt(card, regeneration_hint)
        seed = deterministic_seed(card.id, global_attempt)

        extra_negative = concept_negative_terms(card)
        cat_negative = category_negative_terms(card.visual_category)
        negative_chunks = [NEGATIVE_PROMPT] + [n for n in (extra_negative, cat_negative) if n]
        full_negative = ", ".join(negative_chunks)

        crop_config = _portrait_crop_config(card)
        gen_kwargs = {}
        if crop_config:
            gen_kwargs = {"width": crop_config["gen_width"], "height": crop_config["gen_height"]}

        generated = generate_image_api(
            prompt=prompt,
            negative_prompt=full_negative,
            filepath=filepath,
            seed=seed,
            **gen_kwargs,
        )

        if not generated:
            log.error("[%s] Image generation failed", card.id)
            result.status = "Failed_Generation"
            break

        if crop_config:
            try:
                _apply_portrait_crop(filepath, crop_config)
                log.info("[%s] applied portrait crop to %s", card.id, crop_config["crop_to"])
            except Exception as e:
                log.warning("[%s] portrait crop failed (non-fatal, using uncropped): %s", card.id, e)

        _display_inline(filepath, card.id, global_attempt)

        try:
            passed, failed_criteria, reasoning, fix, model_used = perform_image_qa(filepath, card)

        except VisionQANotConfigured as e:
            log.error("[%s] Vision QA not configured: %s", card.id, e)
            result.status = "QA_Unavailable"
            result.qa_history.append({"attempt": global_attempt, "status": "QA_UNAVAILABLE", "reason": str(e)})
            break

        except VisionQAUnavailable as e:
            # Изображение сгенерировано, но проверить его не удалось —
            # не тратим ещё попытки генерации впустую.
            log.error("[%s] Vision QA unavailable: %s", card.id, e)
            result.status = "QA_Unavailable"
            result.qa_history.append({"attempt": global_attempt, "status": "QA_UNAVAILABLE", "reason": str(e)})
            break

        result.qa_history.append({
            "attempt": global_attempt,
            "status": "PASS" if passed else "FAIL",
            "failed_criteria": failed_criteria,
            "reasoning": reasoning,
            "regeneration_instructions": fix,
            "vision_model": model_used,
        })

        if passed:
            log.info("[%s] QA PASS — %s", card.id, reasoning)
            result.status = "Ready"
            break

        log.warning("[%s] QA FAIL — %s — %s", card.id, ", ".join(failed_criteria), reasoning)

        if not fix:
            fix = "Correct the failed QA criteria: " + ", ".join(failed_criteria)
        regeneration_hint = fix
        result.status = "Failed_QA"

    # Переименовываем итоговый файл, дописывая финальный статус в имя —
    # теперь по одному взгляду на папку видно READY/FAILED_QA/... без
    # открытия EXPORT_JSON_PATH. Единая точка после цикла — общая для
    # всех путей выхода (Ready/Failed_QA/Failed_Generation/QA_Unavailable).
    status_tag = result.status.upper()
    final_filepath = os.path.join(OUTPUT_DIR, f"{base_filename}_{status_tag}.png")
    if os.path.exists(filepath) and filepath != final_filepath:
        try:
            if os.path.exists(final_filepath):
                os.remove(final_filepath)
            os.replace(filepath, final_filepath)
            filepath = final_filepath
        except OSError as e:
            log.warning("[%s] could not rename output file to reflect final status: %s", card.id, e)

    if result.status == "Ready" and os.path.exists(filepath):
        result.asset_path = f"Assets/Generated/{os.path.basename(filepath)}"

    return result

# ============================================================
# 20.1 DEBUG: GENERATE-ONLY PREVIEW (NO QA)
# ============================================================
# Быстрая проверка "что вообще умеет рисовать ImageAPI по новому промпту",
# не тратя запросы к Vision QA на заведомо неверную композицию. Полезно
# после правки concept_visual_guidance — сначала смотрим 2-3 сырых
# генерации глазами, и только когда результат выглядит разумно, гоняем
# полный пайплайн с QA.

def generate_preview(card_id: str, n: int = 3) -> None:
    matching = [c for c in validated_cards if c["id"] == card_id]
    if not matching:
        raise ValueError(f"Card '{card_id}' not found in validated_cards")
    card = Card.from_dict(matching[0])

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for i in range(1, n + 1):
        filepath = os.path.join(OUTPUT_DIR, f"{card.id}_preview_{i}.png")
        prompt = build_prompt(card)
        seed = deterministic_seed(f"{card.id}_preview", i)

        extra_negative = concept_negative_terms(card)
        cat_negative = category_negative_terms(card.visual_category)
        negative_chunks = [NEGATIVE_PROMPT] + [n for n in (extra_negative, cat_negative) if n]
        full_negative = ", ".join(negative_chunks)

        log.info("[preview %d/%d] generating %s...", i, n, card.id)
        ok = generate_image_api(prompt, full_negative, filepath, seed=seed)
        if ok:
            _display_inline(filepath, f"{card.id} (preview)", i)
        else:
            log.error("[preview %d/%d] generation failed", i, n)


# ============================================================
# 20.5 DIAGNOSTIC: PROMPT-LENGTH ABLATION
# ============================================================
# Гипотеза (после разбора QA-фейлов prop_3dglasses_01 / char_director_01):
# полный промпт (~1.2-1.6К символов, MAIN SUBJECT + STYLE + concept
# guidance + CATEGORY_RULES + no-crop clause) может быть слишком длинным
# и составным для бесплатной `zimage` — модель хватает 2-3 самых заметных
# слова (цвет, "glasses", "portrait") и роняет остальные инструкции
# (изоляция объекта, студийный фон, камера и т.д.), а не банально режет
# URL по длине.
#
# Проверяем это ПРАВИЛЬНЫМ ablation-экспериментом: генерируем 4 версии
# промпта возрастающей длины (A -> D, где D == текущий продакшн-промпт),
# ФИКСИРУЯ seed и negative_prompt одинаковыми для всех уровней одной
# карты. Так любая разница в композиции объясняется ИМЕННО длиной/
# сложностью позитивного промпта, а не случайностью seed.
#
# Не автоматизируем вывод "лучше/хуже" — глазами быстрее увидеть, на
# каком уровне модель "теряет нить", чем писать эвристику для этого.

def _prompt_ablation_levels(card) -> dict:
    no_text_clause = (
        "Strong single focal point, subject fully visible, no cropping. "
        "No text, letters, numbers, logos, or watermarks anywhere."
    )
    style_clause = (
        f"STYLE: {MASTER_STYLE}, flat stylized cartoon illustration, "
        "not photorealistic, not a 3D render."
    )

    level_a = f"MAIN SUBJECT: {card.concept}\n\n{no_text_clause}"
    level_b = f"MAIN SUBJECT: {card.concept}\n\n{style_clause}\n\n{no_text_clause}"
    level_c = (
        f"MAIN SUBJECT: {card.concept}\n\n{style_clause}\n\n"
        f"{concept_visual_guidance(card)}\n\n{no_text_clause}"
    )
    level_d = build_prompt(card)  # текущий продакшн-промпт как есть, для сравнения

    return {"A_minimal": level_a, "B_plus_style": level_b,
            "C_plus_guidance": level_c, "D_full_production": level_d}


def diagnose_prompt_ablation(card_id: str) -> None:
    matching = [c for c in validated_cards if c["id"] == card_id]
    if not matching:
        raise ValueError(f"Card '{card_id}' not found in validated_cards")
    card = Card.from_dict(matching[0])

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Один и тот же seed и negative для всех уровней — единственная
    # переменная, которая меняется, это длина/состав POSITIVE промпта.
    seed = deterministic_seed(f"{card.id}_ablation", 1)
    extra_negative = concept_negative_terms(card)
    cat_negative = category_negative_terms(card.visual_category)
    full_negative = ", ".join(
        [NEGATIVE_PROMPT] + [n for n in (extra_negative, cat_negative) if n]
    )

    log.info("[ablation] card=%s seed=%d (fixed across all levels)", card.id, seed)

    for level_name, prompt in _prompt_ablation_levels(card).items():
        filepath = os.path.join(OUTPUT_DIR, f"{card.id}_ablation_{level_name}.png")
        log.info("[ablation] %-18s | %4d chars | %s",
                  level_name, len(prompt), prompt.replace("\n", " ")[:100] + "...")

        ok = generate_image_api(prompt, full_negative, filepath, seed=seed)
        if ok:
            _display_inline(filepath, f"{card.id} — {level_name} ({len(prompt)} chars)", 0)
        else:
            log.error("[ablation] %s generation failed", level_name)


def diagnose_seed_sweep(card_id: str, n_seeds: int = 6) -> None:
    """
    В отличие от diagnose_prompt_ablation (где seed ФИКСИРОВАН, а меняется
    промпт), здесь ФИКСИРОВАН промпт (текущий полный build_prompt() — то,
    что реально пойдёт в прод), а меняется seed.

    Зачем: если несколько версий промпта подряд дают на ОДНОМ и том же
    ablation-seed похожий провал (см. лог — та же сетка голов пятый раз
    подряд на seed=188791833 несмотря на переписанный промпт), нельзя
    понять, промпт всё ещё плохой или просто у этого конкретного seed'а
    изначальный шум сам по себе несёт сеточную/повторяющуюся структуру,
    которую никаким текстом не перебить. Этот тест разводит две гипотезы:
    показывает реальное распределение результатов ТЕКУЩЕГО промпта по
    разным seed'ам, а не судьбу одного (возможно неудачного) броска.
    """
    matching = [c for c in validated_cards if c["id"] == card_id]
    if not matching:
        raise ValueError(f"Card '{card_id}' not found in validated_cards")
    card = Card.from_dict(matching[0])

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Один и тот же (текущий продакшн) промпт для всех — единственная
    # переменная, которая меняется, это seed.
    prompt = build_prompt(card)
    extra_negative = concept_negative_terms(card)
    cat_negative = category_negative_terms(card.visual_category)
    full_negative = ", ".join(
        [NEGATIVE_PROMPT] + [n for n in (extra_negative, cat_negative) if n]
    )

    log.info(
        "[seed_sweep] card=%s — %d different seeds, SAME full production prompt (%d chars)",
        card.id, n_seeds, len(prompt),
    )

    for i in range(1, n_seeds + 1):
        # "_sweep" в качестве соли — гарантированно другое пространство
        # seed'ов, чем прод (attempt_offset-последовательность) и чем
        # ablation ("_ablation"), чтобы разные диагностики не пересекались.
        seed = deterministic_seed(f"{card.id}_sweep", i)
        filepath = os.path.join(OUTPUT_DIR, f"{card.id}_sweep_{i}.png")
        log.info("[seed_sweep] %d/%d seed=%d", i, n_seeds, seed)

        ok = generate_image_api(prompt, full_negative, filepath, seed=seed)
        if ok:
            _display_inline(filepath, f"{card.id} — sweep #{i} (seed {seed})", 0)
        else:
            log.error("[seed_sweep] #%d generation failed", i)


# ============================================================
# 21. ORCHESTRATOR
# ============================================================


# Максимум времени ожидания результата ОДНОЙ карты в run_production_pipeline.
# Это верхний предел на весь путь одной карты: несколько QA-итераций,
# несколько vision-моделей с фолбэком, каждая — с собственным hard-timeout.
# Если карта не уложилась — считаем её потерянной и идём дальше, вместо
# того чтобы весь пайплайн ждал её неопределённо долго.
PER_CARD_TIMEOUT_SECONDS = float(os.environ.get("PER_CARD_TIMEOUT_SECONDS", "600"))
MAX_CONSECUTIVE_QA_UNAVAILABLE = int(os.environ.get("MAX_CONSECUTIVE_QA_UNAVAILABLE", "3"))


def _load_status_database() -> dict:
    """
    Читает EXPORT_JSON_PATH (если он существует) и возвращает
    {card_id: entry} по всем картам, сохранённым в предыдущих прогонах.

    Источник состояния для resume/skip (ТЗ §0.9 "Кэширование/resume").
    Файл может отсутствовать (первый запуск) или быть повреждён
    (прерванная запись) — в обоих случаях просто стартуем с нуля,
    а не роняем пайплайн.
    """
    if not os.path.exists(EXPORT_JSON_PATH):
        return {}
    try:
        with open(EXPORT_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.warning(
            "Could not read existing status file %s (%s) — "
            "treating as no prior state",
            EXPORT_JSON_PATH, e,
        )
        return {}
    return {entry["id"]: entry for entry in data.get("generated_cards", []) if "id" in entry}


def run_production_pipeline(raw_cards: list[dict], resume: bool = True) -> None:
    """
    resume=True (по умолчанию): карты, уже помеченные "Ready" в
    EXPORT_JSON_PATH от предыдущего прогона, пропускаются — не тратим
    заново QA/image-API запросы на уже провалидированные карты. Их
    сохранённая запись (asset_path, qa_history и т.д.) переносится в
    итоговый файл как есть.

    resume=False: старое поведение — обрабатываем все переданные карты
    заново, игнорируя предыдущий статус (полезно, например, если
    поменялся MASTER_STYLE/CATEGORY_RULES и старые "Ready"-ассеты нужно
    перегенерировать).

    Карты со статусом, отличным от "Ready" (Failed_QA, Failed_Generation,
    QA_Unavailable, Timed_Out), НЕ пропускаются — они обрабатываются
    заново на каждом прогоне, пока не станут "Ready" или не будут
    отфильтрованы явно.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    export_dir = os.path.dirname(EXPORT_JSON_PATH)
    if export_dir:
        os.makedirs(export_dir, exist_ok=True)

    prior_status = _load_status_database() if resume else {}

    export_database = []
    cards_to_process = []  # list of (Card, attempt_offset)
    skipped_ids = []

    for raw in raw_cards:
        card_id = raw.get("id")
        prior_entry = prior_status.get(card_id) if card_id else None

        if resume and prior_entry and prior_entry.get("status") == "Ready":
            # Уже готова и провалидирована в прошлом прогоне — переносим
            # её запись как есть, никаких новых вызовов API для неё.
            export_database.append(prior_entry)
            skipped_ids.append(card_id)
            continue

        # Если карта раньше уже пыталась (Failed_QA/Failed_Generation/
        # QA_Unavailable), продолжаем сквозной счётчик попыток с того
        # места, где остановились — это и есть "гибкий seed": следующий
        # прогон исследует НОВЫЕ seed'ы, а не пересдаёт старые.
        attempt_offset = prior_entry.get("attempts", 0) if (resume and prior_entry) else 0

        cards_to_process.append((Card.from_dict(raw), attempt_offset))

    if skipped_ids:
        log.info(
            "Resume: skipping %d already-Ready card(s): %s",
            len(skipped_ids), ", ".join(skipped_ids),
        )

    log.info(
        "Starting pipeline: %d card(s) to process, %d skipped (already Ready)",
        len(cards_to_process), len(skipped_ids),
    )

    if cards_to_process:
        # ВАЖНО: НЕ используем "with ThreadPoolExecutor(...) as pool:" — при
        # выходе из with-блока Python безусловно вызывает shutdown(wait=True),
        # который ждёт завершения воркер-потоков БЕЗ таймаута (см. трейсбек:
        # именно тут случилось "зависание", хотя каждый отдельный сетевой вызов
        # уже был ограничен hard-timeout'ом). Управляем пулом вручную, чтобы
        # иметь возможность прекратить ожидание и выйти, даже если конкретная
        # карта не уложилась в разумное время.
        #
        # ИСПРАВЛЕНО (масштаб коллекции — 160 карт вместо 2 тестовых):
        # раньше ВСЕ карты отправлялись в pool.submit() разом одним циклом
        # ("futures = {pool.submit(...): card for card in cards_to_process}").
        # Проблема: с MAX_PARALLEL_CARDS=1 воркер-поток всё равно продолжает
        # молча разбирать уже отправленную очередь в фоне, ДАЖЕ если мы
        # прекратили читать результаты в цикле ниже — то есть "остановиться
        # пораньше" не останавливало реальную работу (GPU-генерацию), а
        # только переставало её показывать. На 2 картах это было незаметно,
        # на 160 — реальный риск спалить GPU-квоту на карты, которые всё
        # равно не пройдут QA (см. circuit breaker ниже). Теперь отправляем
        # карты СКОЛЬЗЯЩИМИ пачками по MAX_PARALLEL_CARDS штук, проверяя
        # условие остановки МЕЖДУ пачками — то есть невзятые в работу карты
        # реально остаются нетронутыми.
        #
        # Упрощение (честно, а не молча): при MAX_PARALLEL_CARDS==1 (как в
        # текущем конфиге, "для бесплатного тарифа — последовательно") это
        # эквивалентно прежнему поведению один-к-одному. При
        # MAX_PARALLEL_CARDS>1 это чуть менее эффективно, чем "стартовать
        # следующую карту сразу как освободился слот" (мы ждём завершения
        # ВСЕЙ пачки перед отправкой следующей) — сознательный компромисс
        # ради простоты и корректности circuit breaker'а.
        pool = ThreadPoolExecutor(max_workers=MAX_PARALLEL_CARDS)
        consecutive_qa_unavailable = 0
        circuit_broken = False
        pending = list(cards_to_process)
        total_to_process = len(pending)
        processed_count = 0

        try:
            while pending and not circuit_broken:
                batch = pending[:MAX_PARALLEL_CARDS]
                del pending[:MAX_PARALLEL_CARDS]

                futures = {
                    pool.submit(process_card, card, attempt_offset): card
                    for card, attempt_offset in batch
                }

                # ВАЖНО: НЕ используем "for future in as_completed(futures):" —
                # as_completed() сам по себе блокируется внутри в ожидании хотя бы
                # одного завершённого future, БЕЗ таймаута на этом уровне. Даже с
                # future.result(timeout=...) внутри тела цикла это не спасает: тело
                # цикла просто никогда не достигается, если ни один future не
                # завершился. Перебираем словарь напрямую — так каждый вызов
                # .result(timeout=...) независимо ограничен по времени, независимо
                # от состояния остальных future.
                for future, card in futures.items():
                    try:
                        result = future.result(timeout=PER_CARD_TIMEOUT_SECONDS)
                    except TimeoutError:
                        log.error(
                            "[%s] exceeded PER_CARD_TIMEOUT_SECONDS=%.0fs — "
                            "abandoning this card, its worker thread may still be "
                            "running in the background",
                            card.id, PER_CARD_TIMEOUT_SECONDS,
                        )
                        result = CardResult(card=card, status="Timed_Out")
                    except Exception:
                        log.exception("[%s] unhandled pipeline error", card.id)
                        result = CardResult(card=card, status="Failed_Generation")

                    entry = {
                        "id": result.card.id,
                        "set_id": result.card.set_id,
                        "type": result.card.type,
                        "rarity": result.card.rarity,
                        "visual_category": result.card.visual_category,
                        "concept": result.card.concept,
                        "status": result.status,
                        "attempts": result.attempts,
                    }
                    if result.asset_path:
                        entry["asset_path"] = result.asset_path
                    if result.qa_history:
                        entry["qa_history"] = result.qa_history

                    export_database.append(entry)
                    processed_count += 1
                    log.info("[%s] FINAL STATUS: %s", card.id, result.status)

                    if result.status == "QA_Unavailable":
                        consecutive_qa_unavailable += 1
                    else:
                        consecutive_qa_unavailable = 0

                    if consecutive_qa_unavailable >= MAX_CONSECUTIVE_QA_UNAVAILABLE:
                        circuit_broken = True
                        untouched = total_to_process - processed_count
                        log.error(
                            "[Pipeline] %d QA failures in a row — likely daily QA "
                            "quota exhausted (or upstream provider down). Stopping "
                            "early to avoid burning GPU time generating images that "
                            "can't be graded right now. %d card(s) left untouched — "
                            "rerun with resume=True later (e.g. after the daily "
                            "reset) to pick them up.",
                            consecutive_qa_unavailable, untouched,
                        )
                        break
        finally:
            # wait=False — принципиально: если какой-то воркер реально завис,
            # мы всё равно не блокируем возврат управления пользователю.
            pool.shutdown(wait=False)

    _atomic_json_write(EXPORT_JSON_PATH, {"generated_cards": export_database})

    ready = sum(1 for e in export_database if e["status"] == "Ready")
    failed_qa = sum(1 for e in export_database if e["status"] == "Failed_QA")
    failed_generation = sum(1 for e in export_database if e["status"] == "Failed_Generation")
    qa_unavailable = sum(1 for e in export_database if e["status"] == "QA_Unavailable")

    log.info("================================================")
    log.info("Pipeline finished")
    log.info("Ready: %d/%d", ready, len(export_database))
    log.info("Failed_QA: %d", failed_qa)
    log.info("Failed_Generation: %d", failed_generation)
    log.info("QA_Unavailable: %d", qa_unavailable)
    if skipped_ids:
        log.info("Skipped (resume, already Ready): %d", len(skipped_ids))
    log.info("Metadata: %s", EXPORT_JSON_PATH)
    log.info("================================================")


# ============================================================
# 22. LLM ORCHESTRATOR — STAGE 1: THEME -> SET-CONCEPT VARIANTS
# ============================================================
# ТЗ §0.1 / §0.9: верхний уровень задачи — "тема (+ свободные пожелания
# продюсера) -> несколько готовых вариантов сета на выбор". Это ЧИСТО
# ТЕКСТОВАЯ стадия: ни одного обращения к image-API. Продюсер выбирает
# 1 вариант из N, и только выбранный вариант (`variant["cards"]`) идёт
# дальше в run_production_pipeline() как raw_cards.
#
# ТЗ §0.3: тестовые/промежуточные сеты собираются БЕЗ золотых карточек —
# оркестратор просит модель генерировать только категории 1-4 и жёстко
# отбраковывает любую карту с visual_category == 5 или rarity == "gold"
# на этапе валидации ответа (защита от того, что текстовая модель всё
# равно вставит золотую карту, несмотря на инструкцию).
#
# ТЗ §0.9: разбивка категорий 3-3-2-2 на 10 карт — это НАШ дефолт для
# затравки промпта, а не дословное требование заказчика (в тексте
# задания только "присутствуют все 4 категории", без точных квот).
# Отмечено явно здесь и в самом системном промпте, чтобы не повторить
# ошибку "выдать своё допущение за требование" (см. п.0.9 и п.0.10 в ТЗ).

ORCHESTRATOR_DEFAULT_NUM_VARIANTS = int(os.environ.get("ORCHESTRATOR_NUM_VARIANTS", "3"))
ORCHESTRATOR_DEFAULT_CARDS_PER_SET = int(os.environ.get("ORCHESTRATOR_CARDS_PER_SET", "10"))
# ДОБАВЛЕНО (v32, реальный баг из диалога): до этого фикса
# generate_set_concepts()/render_set_variants() не сохраняли Stage-1
# результат вообще — ЛЮБОЙ повторный вызов (после обрыва по квоте,
# перезапуска ядра, любой причины) генерировал ВСЕ варианты заново с
# нуля, с НОВЫМИ id карт (LLM не детерминирован). Старые уже
# отрендеренные Ready-картинки при этом не терялись физически, но
# становились сиротами: новые id не совпадали со старыми, resume в
# run_production_pipeline их не узнавал, а в READY_OUTPUT_DIR копились
# карты из разных, несовместимых по концепции прогонов вперемешку — то,
# что и было видно на скриншоте (>10 разных предметов под одной темой).
# generate_collection() эту же проблему решает для полной коллекции
# через COLLECTION_EXPORT_JSON_PATH — но для однo-сетового пути
# (render_set_variants, добавлен позже, в v28) аналогичного Stage-1
# resume не было добавлено. Симметрично чинится тем же способом.
SET_VARIANTS_EXPORT_JSON_PATH = os.environ.get(
    "SET_VARIANTS_EXPORT_JSON_PATH", "/kaggle/working/configs/set_variants_export_data.json"
)


def _load_set_variants_cache() -> dict:
    if not os.path.exists(SET_VARIANTS_EXPORT_JSON_PATH):
        return {}
    try:
        with open(SET_VARIANTS_EXPORT_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.warning("[SetVariants] could not read %s (%s) — starting fresh", SET_VARIANTS_EXPORT_JSON_PATH, e)
        return {}


def _save_set_variants_cache(data: dict) -> None:
    _atomic_json_write(SET_VARIANTS_EXPORT_JSON_PATH, data)


# Нет захардкоженного дефолта модели: бесплатные text-модели на OpenRouter
# часто переименовываются/выводятся из ротации (тот же паттерн, что и с
# VISION_MODEL в конфиг-ячейке) — лучше явная ошибка конфигурации, чем
# молчаливая генерация с моделью, которую разработчик не выбирал.
ORCHESTRATOR_API_MIN_INTERVAL_SECONDS = float(os.environ.get("ORCHESTRATOR_API_MIN_INTERVAL_SECONDS", "4.0"))
ORCHESTRATOR_API_TIMEOUT_SECONDS = float(os.environ.get("ORCHESTRATOR_API_TIMEOUT_SECONDS", "60"))
# То же исправление, что у VISION_MAX_TOKENS — см. комментарий там.
ORCHESTRATOR_MAX_TOKENS = int(os.environ.get("ORCHESTRATOR_MAX_TOKENS", "4000"))
MAX_ORCHESTRATOR_RETRIES = int(os.environ.get("MAX_ORCHESTRATOR_RETRIES", "1"))

_ALLOWED_ORCHESTRATOR_CATEGORIES = (1, 2, 3, 4)  # категория 5 (gold) сюда не входит — см. ТЗ §0.3


def _get_orchestrator_config():
    """
    Отдельные ORCHESTRATOR_API_BASE_URL / ORCHESTRATOR_API_KEY можно не
    задавать: по умолчанию используются те же значения, что и для vision
    QA (VISION_API_BASE_URL / VISION_API_KEY) — обычно это один и тот же
    OpenRouter-аккаунт/ключ, просто с другой (текстовой) моделью.
    ORCHESTRATOR_MODEL при этом обязателен — своего дефолта у него нет.
    """
    base_url = os.environ.get("ORCHESTRATOR_API_BASE_URL") or os.environ.get("VISION_API_BASE_URL", "")
    api_key = os.environ.get("ORCHESTRATOR_API_KEY") or os.environ.get("VISION_API_KEY", "")
    primary = os.environ.get("ORCHESTRATOR_MODEL", "")
    fallbacks_raw = os.environ.get("ORCHESTRATOR_MODEL_FALLBACKS", "")
    fallbacks = [m.strip() for m in fallbacks_raw.split(",") if m.strip()]

    models = []
    if primary:
        models.append(primary)
    for m in fallbacks:
        if m not in models:
            models.append(m)

    return base_url, api_key, models


_orchestrator_api_lock = threading.Lock()
_orchestrator_api_last_call = 0.0


def _throttle_orchestrator_api():
    global _orchestrator_api_last_call
    with _orchestrator_api_lock:
        elapsed = time.monotonic() - _orchestrator_api_last_call
        wait = ORCHESTRATOR_API_MIN_INTERVAL_SECONDS - elapsed
        if wait > 0:
            time.sleep(wait)
        _orchestrator_api_last_call = time.monotonic()


class OrchestratorNotConfigured(RuntimeError):
    pass


class OrchestratorUnavailable(RuntimeError):
    pass


def _build_orchestrator_system_prompt(cards_per_set: int) -> str:
    category_rules_text = "\n".join(
        f"{cat}. {CATEGORY_LABELS[cat]} — {CATEGORY_RULES[cat]}"
        for cat in _ALLOWED_ORCHESTRATOR_CATEGORIES
    )

    return f"""
You are an Art Director / Set Designer for a mobile match-3 collectible
card game. You design TEXT-ONLY card concepts — you never generate or
describe actual images, only what each card should depict.

MASTER VISUAL STYLE (for context only, do not repeat it in your output):
{MASTER_STYLE}

Given a theme (and optional free-form producer notes), propose several
DIFFERENT candidate sets for that theme. Each set must contain exactly
{cards_per_set} card concepts.

Allowed visual categories for this task are ONLY 1-4:
{category_rules_text}

Do NOT use category 5 (gold / story-scene cards) and do NOT use
rarity "gold" for any card in this task — grand/gold sets are assembled
separately, later, from a different process. Every card here must use
rarity "common", "rare", or "epic".

All 4 categories (1, 2, 3, 4) must be represented within every set. A
reasonable starting distribution across {cards_per_set} cards is 3/3/2/2
(categories 1/2/3/4) — but treat this as a flexible default, not a rigid
formula; use your judgement for what fits the theme, as long as all four
categories appear at least once.

Each card concept must be:
- A single self-contained sentence describing one concrete object or
  character (no text/writing/logos should ever appear on the card, so
  never propose a concept that requires readable text, like a ticket
  stub or a movie poster with a title, an engraved monogram, or an
  embossed initial/letter).
- For category 1 specifically: the object itself must be describable
  as a few large clean shapes. Do NOT write category-1 concepts that
  bake in fine surface detail as part of the idea itself — no cracks,
  no engravings, no intricate patterns, no small readable markings, no
  "detailed"/"ornate"/"intricate" as a descriptor. A category-1 concept
  that requires fine detail to be recognizable is a bad fit for that
  category — simplify the object, or move it to category 2/3 instead
  where surface detail is acceptable. Concrete example: a vintage film
  reel (two flanges with wound film visible between them, cut-out
  spokes) is a BAD category-1 choice — it needs visible fine structure
  to read as a film reel rather than a plain wheel, and it belongs in
  category 2 or 3 instead, resting on a surface, not floating.
- For category 2 and 3 specifically: avoid concept language that
  implies a grand, opulent, or fully decorated setting — words like
  "glamorous", "opulent", "grand hall", "ballroom", "palace",
  "elaborate" tend to pull image generation toward a fully-developed
  environment (columns, arches, chandeliers, crowds), which directly
  violates the plain-backdrop / simple-surface requirement for these
  categories. Describe the character or object plainly and let the
  base art style carry the atmosphere — save opulent/fully-realized
  settings for category 4 concepts, where they belong.
- Free of any references to real branded companies, real real-world
  products, or real named public figures.
- Distinct from every other card concept within the same set (no near
  duplicates). If you are asked for more than one candidate set for
  the same theme across separate requests, make each one feel like a
  clearly different sub-angle on the theme, not a rehash of a
  previous one.

For every card, also propose:
- "id": a short unique lowercase snake_case id, prefixed "prop_" for
  objects or "char_" for characters (e.g. "prop_clapperboard_02").
- "type": "object" or "character".
- "rarity": "common", "rare", or "epic" (never "gold").
- "visual_category": integer 1-4.

Return STRICT JSON only, no markdown fences, no commentary, matching
exactly this shape:

{{
  "variants": [
    {{
      "variant_label": "short human-readable name for this set's sub-angle",
      "cards": [
        {{
          "id": "...",
          "type": "object",
          "rarity": "common",
          "visual_category": 1,
          "concept": "..."
        }}
      ]
    }}
  ]
}}
""".strip()


def _request_single_set_variant(
    theme: str,
    producer_notes: str,
    n_cards: int,
    system_prompt: str,
    base_url: str,
    api_key: str,
    models: list[str],
    set_id_prefix: str,
    v_index: int,
) -> dict:
    """
    Один HTTP-запрос (с полным перебором fallback-моделей) на РОВНО ОДИН
    вариант сета. Вынесено из generate_set_concepts() отдельной функцией
    (см. её докстринг про ИСПРАВЛЕНО v28 — почему теперь всегда по 1
    варианту за вызов, а не N за один).
    """
    user_prompt = f"THEME: {theme}\n\n"
    if producer_notes:
        user_prompt += f"PRODUCER NOTES (free-form wishes): {producer_notes}\n\n"
    user_prompt += (
        f"Propose exactly 1 candidate set for this theme, with exactly "
        f"{n_cards} card concepts, following the rules and JSON shape "
        f"from the system prompt (still return it wrapped in a "
        f"single-element \"variants\" list, per the schema)."
    )

    last_error = None

    for model_index, model in enumerate(models):
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.9,  # чуть выше vision QA (temperature=0) — нужно разнообразие между вариантами
            "max_tokens": ORCHESTRATOR_MAX_TOKENS,
        }

        try:
            _throttle_orchestrator_api()
            log.info("[Orchestrator] model '%s' — requesting variant #%d for theme %r",
                      model, v_index, theme)

            response = _call_with_hard_timeout(
                requests.post, ORCHESTRATOR_API_TIMEOUT_SECONDS + 10,
                f"{base_url.rstrip('/')}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=ORCHESTRATOR_API_TIMEOUT_SECONDS,
            )

            if response.status_code == 429 or 400 <= response.status_code < 500:
                body = response.text[:500]
                raise OrchestratorUnavailable(
                    f"HTTP {response.status_code} for model '{model}' — {body}"
                )
            response.raise_for_status()

            raw_text = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(_clean_json_response(raw_text))
            variants_raw = parsed.get("variants")
            if not isinstance(variants_raw, list) or not variants_raw:
                raise ValueError("Response JSON has no non-empty 'variants' list")

            variant = variants_raw[0]
            cards_raw = variant.get("cards", [])
            set_id = f"set_{set_id_prefix}_v{v_index}"

            clean_cards = []
            for card in cards_raw:
                category = card.get("visual_category")
                rarity = card.get("rarity", "common")

                # Жёсткая защита от золотых карт на этой стадии (ТЗ §0.3),
                # даже если модель проигнорировала системный промпт.
                if category not in _ALLOWED_ORCHESTRATOR_CATEGORIES or rarity == "gold":
                    log.warning(
                        "[Orchestrator] dropping card '%s' from variant #%d — "
                        "category=%r rarity=%r not allowed at Stage 1",
                        card.get("id", "?"), v_index, category, rarity,
                    )
                    continue

                clean_cards.append({
                    # ИСПРАВЛЕНО (v29, найдено на реальном прогоне — см. диалог):
                    # id больше не берётся у модели "как есть". Каждый вариант
                    # теперь отдельный независимый HTTP-вызов (v28) — модель
                    # физически не видит id, использованные в других вариантах
                    # того же сета, и системный промпт просит лишь "unique id"
                    # В ПРЕДЕЛАХ ОДНОГО ответа, не более того. Без namespacing
                    # два варианта могут случайно назвать разные карты
                    # одинаково (например, оба — "prop_camera_01"), а id — это
                    # одновременно ключ resume/QA-истории И основа имени файла
                    # на диске, так что коллизия тихо перезаписывает файл и
                    # путает QA-историю одной карты историей другой. Префикс
                    # set_id (который уже включает v-индекс) гарантирует
                    # уникальность без надежды на дисциплину модели.
                    "id": f"{set_id}_{card['id']}",
                    "set_id": set_id,
                    "type": card.get("type", "object"),
                    "rarity": rarity,
                    "visual_category": category,
                    "concept": card["concept"],
                })

            if len(clean_cards) < n_cards:
                log.warning(
                    "[Orchestrator] variant #%d ('%s') has only %d/%d usable cards "
                    "after filtering — model output may be incomplete",
                    v_index, variant.get("variant_label", "?"), len(clean_cards), n_cards,
                )

            log.info("[Orchestrator] model '%s' returned variant #%d ('%s') with %d card(s)",
                      model, v_index, variant.get("variant_label", "?"), len(clean_cards))

            return {
                "set_id": set_id,
                "variant_label": variant.get("variant_label", f"Variant {v_index}"),
                "cards": clean_cards,
            }

        except OrchestratorUnavailable as e:
            last_error = e
            # ИСПРАВЛЕНО (по факту из реального лога): раньше здесь не
            # логировалось НИЧЕГО, кроме следующего "switching to
            # fallback" — то, что gemma-4-31b и glm-5.2 отвалились с
            # 4xx/429 в вашем прогоне, было видно только по ОТСУТСТВИЮ
            # строки между "requesting" и "switching", а не по явному
            # сообщению. Теперь причина видна в логе явно.
            log.warning("[Orchestrator] model '%s' unavailable for variant #%d: %s", model, v_index, e)
        except (requests.exceptions.RequestException, json.JSONDecodeError,
                KeyError, TypeError, ValueError) as e:
            last_error = e
            body_snippet = ""
            try:
                body_snippet = f" — raw body: {response.text[:300]!r}"
            except NameError:
                pass
            log.warning("[Orchestrator] invalid/failed response from model '%s' for variant #%d: %s%s",
                        model, v_index, e, body_snippet)

        if model_index < len(models) - 1:
            next_model = models[model_index + 1]
            log.info("[Orchestrator] switching to fallback '%s'", next_model)

    _log_quota_reset_if_present(str(last_error), "Orchestrator")
    raise OrchestratorUnavailable(
        f"All orchestrator models unavailable for variant #{v_index}: {models}. Last error: {last_error}"
    )


def generate_set_concepts(
    theme: str,
    producer_notes: str = "",
    set_id_prefix: str = "set",
    num_variants: Optional[int] = None,
    cards_per_set: Optional[int] = None,
    resume: bool = True,
) -> list[dict]:
    """
    Stage 1 (текст-only, ТЗ §0.9). Принимает тему набора (+ опционально
    свободные пожелания продюсера) и возвращает список из `num_variants`
    вариантов сета, каждый — `cards_per_set` концептов карт в категориях
    1-4, БЕЗ единого обращения к image-API.

    ИСПРАВЛЕНО (v28, по факту реального лога): раньше все `num_variants`
    вариантов запрашивались ОДНИМ HTTP-вызовом ("Propose exactly 3
    different candidate sets... " в одном user_prompt). На практике для
    num_variants=3 это стабильно проваливало ВСЮ цепочку fallback'ов —
    primary упирался в hard-timeout (генерация ~3x объёма JSON занимает
    ощутимо дольше, чем успевает 70-секундный таймаут), а фолбэки
    возвращали 4xx/пустые ответы. При этом тот же самый пайплайн внутри
    generate_collection() всегда просил ПО ОДНОМУ варианту за вызов
    (num_variants=1 для каждого из 15 обычных сетов) — и именно это
    работало устойчиво в реальном прогоне (см. лог: 6+ сетов подряд
    успешно, до упора в дневную квоту). Теперь generate_set_concepts()
    делает то же самое для любого num_variants: N отдельных HTTP-вызовов
    по 1 варианту в каждом, а не один "толстый" вызов. Дороже по числу
    запросов (N вместо 1), но это именно тот размер запроса, который
    подтверждённо работает на бесплатных моделях — и для 1 сета x 3
    варианта (это ТЗ, не вся коллекция) запас квоты это позволяет.

    ДОБАВЛЕНО (v32): Stage-1 resume через SET_VARIANTS_EXPORT_JSON_PATH,
    по аналогии с COLLECTION_EXPORT_JSON_PATH у generate_collection().
    Кэш ключуется по (theme, set_id_prefix) — при resume=True (дефолт)
    уже сгенерированные варианты для ТОЧНО ЭТОЙ темы переиспользуются
    без нового обращения к LLM, генерируются только недостающие индексы
    вариантов. Если num_variants/cards_per_set в кэше отличаются от
    запрошенных сейчас — кэш для этого ключа считается несовместимым и
    игнорируется целиком (не подмешивается частично), с явным warning.
    resume=False принудительно генерирует всё заново и перезаписывает
    кэш — используйте, если старые варианты нужно осознанно выбросить
    (например, тема осталась той же, но её концептуально пересматривают).

    Резюмируемость по вариантам: если один из вариантов не удаётся
    сгенерировать после полного перебора fallback-моделей — это НЕ
    роняет остальные (по аналогии с `generate_collection`'s missing_sets
    на уровне сетов); функция просто вернёт меньше вариантов, чем
    просили, залогировав явное предупреждение. Уже успешно сгенерированные
    в ЭТОМ вызове варианты сохраняются в кэш сразу по готовности (не
    только в конце) — обрыв на варианте #2 не теряет вариант #1.

    Возвращаемая структура:
    [
        {
            "set_id": "set_<prefix>_v1",
            "variant_label": "...",
            "cards": [
                {"id", "set_id", "type", "rarity", "visual_category", "concept"},
                ...
            ],
        },
        ...
    ]

    Каждый элемент `variant["cards"]` уже в формате, который напрямую
    принимает `run_production_pipeline()` (или `Card.from_dict`) — так
    продюсер после выбора варианта передаёт `variants[i]["cards"]`
    дальше в Stage 2 без дополнительного маппинга полей.
    """
    base_url, api_key, models = _get_orchestrator_config()
    if not (base_url and api_key and models):
        raise OrchestratorNotConfigured(
            "LLM Orchestrator is not configured — set ORCHESTRATOR_MODEL "
            "(and optionally ORCHESTRATOR_API_BASE_URL / ORCHESTRATOR_API_KEY "
            "if different from the vision QA account)."
        )

    n_variants = num_variants or ORCHESTRATOR_DEFAULT_NUM_VARIANTS
    n_cards = cards_per_set or ORCHESTRATOR_DEFAULT_CARDS_PER_SET

    system_prompt = _build_orchestrator_system_prompt(n_cards)

    cache_key = f"{theme}|{set_id_prefix}"
    full_cache = _load_set_variants_cache()
    cached_entry = full_cache.get(cache_key) if resume else None

    cached_by_index: dict[int, dict] = {}
    if cached_entry:
        if cached_entry.get("num_variants") != n_variants or cached_entry.get("cards_per_set") != n_cards:
            log.warning(
                "[SetVariants] cache for theme %r/%r found but num_variants/cards_per_set "
                "differ (cached %s/%s vs requested %s/%s) — ignoring stale cache, generating fresh",
                theme, set_id_prefix, cached_entry.get("num_variants"),
                cached_entry.get("cards_per_set"), n_variants, n_cards,
            )
        else:
            for v in cached_entry.get("variants", []):
                v_index = v.get("v_index")
                if v_index:
                    cached_by_index[v_index] = v["variant"]
            if cached_by_index:
                log.info(
                    "[SetVariants] resume: reusing %d/%d already-generated variant(s) for theme %r",
                    len(cached_by_index), n_variants, theme,
                )

    variants: list[dict] = []
    for v_index in range(1, n_variants + 1):
        if v_index in cached_by_index:
            log.info("[SetVariants] resume: skipping variant #%d (already generated)", v_index)
            variants.append(cached_by_index[v_index])
            continue
        try:
            variant = _request_single_set_variant(
                theme=theme,
                producer_notes=producer_notes,
                n_cards=n_cards,
                system_prompt=system_prompt,
                base_url=base_url,
                api_key=api_key,
                models=models,
                set_id_prefix=set_id_prefix,
                v_index=v_index,
            )
            variants.append(variant)
            cached_by_index[v_index] = variant
            # Сохраняем сразу по готовности каждого варианта, не в конце —
            # обрыв на варианте #2 (квота/сбой) не теряет вариант #1.
            full_cache[cache_key] = {
                "num_variants": n_variants,
                "cards_per_set": n_cards,
                "variants": [
                    {"v_index": idx, "variant": v} for idx, v in sorted(cached_by_index.items())
                ],
            }
            _save_set_variants_cache(full_cache)
        except OrchestratorUnavailable as e:
            log.error(
                "[Orchestrator] variant #%d/%d for theme %r could not be generated "
                "(all models exhausted) — skipping it, continuing with remaining "
                "variants: %s", v_index, n_variants, theme, e,
            )

    if not variants:
        raise OrchestratorUnavailable(
            f"All {n_variants} variant(s) failed for theme {theme!r} — "
            f"see per-variant warnings above for the actual per-model errors."
        )

    if len(variants) < n_variants:
        log.warning(
            "[Orchestrator] theme %r — only %d/%d variant(s) generated successfully",
            theme, len(variants), n_variants,
        )
    else:
        log.info("[Orchestrator] theme %r — all %d variant(s) generated successfully",
                  theme, len(variants))

    return variants


# ============================================================
# 22.5 TOP-LEVEL COLLECTION ORCHESTRATOR (ТЗ §0.2)
# ============================================================
# ТЗ §0.2: "Коллекция = 16 сетов по 10 карт. Сет 16 — гранд, только
# золотые карточки, тема кульминационная. Остальные 15 сетов —
# тематические подтемы коллекции, должны быть разнообразными и не
# повторяться. Внутри каждого обычного сета — микс категорий 1-4."
#
# Этого слоя раньше не существовало вообще: generate_set_concepts()
# (см. выше) — это Stage 1 для ОДНОГО сета с вариантами на выбор
# продюсера. Здесь — оркестрация НАД ним: тема коллекции -> 15 разных
# подтем -> по сету на каждую -> плюс отдельно устроенный гранд-сет.
#
# ВАЖНОЕ ДОПУЩЕНИЕ (аналогично 3-3-2-2 в §0.9 — фиксирую явно, чтобы не
# повторить ту же ошибку "выдать своё решение за требование заказчика"):
# для 15 обычных сетов беру ПО ОДНОМУ варианту на подтему
# (num_variants=1), а не 3-5, как для одиночного сета в §0.9. Причина —
# не эстетика, а квота: OpenRouter free-tier — 50 запросов/сутки НА
# АККАУНТ, ОБЩИЙ для всех бесплатных моделей разом (это уже
# зафиксировано на практике — см. лог "free-models-per-day", где и
# vision QA, и orchestrator ловили один и тот же дневной лимит).
# 15 подтем × 3-5 вариантов утроило/упятерило бы число LLM-вызовов и
# гарантированно исчерпало бы дневную квоту за один прогон сборки
# коллекции. Если варианты на подтему нужны — дергать
# generate_set_concepts(theme=<конкретная подтема>) точечно самому,
# а не через generate_collection().

ORCHESTRATOR_NUM_REGULAR_SETS = int(os.environ.get("ORCHESTRATOR_NUM_REGULAR_SETS", "15"))
COLLECTION_EXPORT_JSON_PATH = os.environ.get(
    "COLLECTION_EXPORT_JSON_PATH", "/kaggle/working/configs/collection_export_data.json"
)


def _build_subtheme_names_system_prompt(n_regular_sets: int) -> str:
    return f"""
You are a Set Designer for a mobile match-3 collectible card game, planning
a full collection's structure.

Given a COLLECTION theme (and optional producer notes), propose exactly
{n_regular_sets} DIFFERENT sub-theme names for the regular sets in this
collection, plus one separate CLIMACTIC finale sub-theme for the
collection's single grand (all-gold) set.

Requirements for the {n_regular_sets} regular sub-themes:
- Each must be a distinct, specific angle on the collection theme (e.g.
  for a "Summer" collection: "Beach", "Garden", "Sports" — not vague
  restatements of the main theme).
- No two sub-themes may repeat or heavily overlap in subject matter.
- Together they should give a sense of covering the collection's theme
  from many different, varied angles.

Requirement for the grand finale sub-theme:
- It should read as the climactic, celebratory high point of the whole
  collection (e.g. for "Summer": "Summer Party") — distinct from all
  {n_regular_sets} regular sub-themes.

Return STRICT JSON only, no markdown fences, no commentary, matching
exactly this shape:

{{
  "regular_subthemes": ["...", "...", ...],
  "grand_subtheme": "..."
}}
""".strip()


def generate_subtheme_names(
    collection_theme: str,
    producer_notes: str = "",
    n_regular_sets: Optional[int] = None,
) -> dict:
    """
    Один лёгкий LLM-вызов: тема коллекции -> N разных непересекающихся
    подтем для обычных сетов + 1 кульминационная подтема для гранд-сета.

    Возвращает {"regular_subthemes": [...], "grand_subtheme": "..."}.
    """
    base_url, api_key, models = _get_orchestrator_config()
    if not (base_url and api_key and models):
        raise OrchestratorNotConfigured(
            "LLM Orchestrator is not configured — set ORCHESTRATOR_MODEL "
            "(and optionally ORCHESTRATOR_API_BASE_URL / ORCHESTRATOR_API_KEY)."
        )

    n_sets = n_regular_sets or ORCHESTRATOR_NUM_REGULAR_SETS
    system_prompt = _build_subtheme_names_system_prompt(n_sets)

    user_prompt = f"COLLECTION THEME: {collection_theme}\n\n"
    if producer_notes:
        user_prompt += f"PRODUCER NOTES (free-form wishes): {producer_notes}\n\n"
    user_prompt += f"Propose {n_sets} regular sub-themes + 1 grand finale sub-theme, per the system prompt."

    last_error = None
    for model_index, model in enumerate(models):
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.9,
            "max_tokens": ORCHESTRATOR_MAX_TOKENS,
        }
        try:
            _throttle_orchestrator_api()
            log.info("[Collection] requesting %d sub-themes for %r (model '%s')",
                      n_sets, collection_theme, model)

            response = _call_with_hard_timeout(
                requests.post, ORCHESTRATOR_API_TIMEOUT_SECONDS + 10,
                f"{base_url.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=ORCHESTRATOR_API_TIMEOUT_SECONDS,
            )
            if response.status_code == 429 or 400 <= response.status_code < 500:
                raise OrchestratorUnavailable(f"HTTP {response.status_code} for model '{model}' — {response.text[:500]}")
            response.raise_for_status()

            raw_text = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(_clean_json_response(raw_text))

            regular = parsed.get("regular_subthemes")
            grand = parsed.get("grand_subtheme")
            if not isinstance(regular, list) or not regular or not grand:
                raise ValueError("Response JSON missing 'regular_subthemes' or 'grand_subtheme'")

            # Дедупликация на всякий случай — модель иногда всё же
            # повторяется, несмотря на явную инструкцию.
            seen = set()
            deduped = []
            for name in regular:
                key = name.strip().lower()
                if key and key not in seen:
                    seen.add(key)
                    deduped.append(name)

            if len(deduped) < n_sets:
                log.warning(
                    "[Collection] model returned only %d unique sub-themes (asked for %d) "
                    "— will reuse/pad if needed at assembly time",
                    len(deduped), n_sets,
                )

            log.info("[Collection] got %d unique sub-themes + grand theme %r", len(deduped), grand)
            return {"regular_subthemes": deduped, "grand_subtheme": grand}

        except OrchestratorUnavailable as e:
            last_error = e
        except (requests.exceptions.RequestException, json.JSONDecodeError,
                KeyError, TypeError, ValueError) as e:
            last_error = e
            log.warning("[Collection] invalid/failed response from model '%s': %s", model, e)

        if model_index < len(models) - 1:
            log.info("[Collection] switching to fallback '%s'", models[model_index + 1])

    _log_quota_reset_if_present(str(last_error), "Collection: sub-theme names")
    raise OrchestratorUnavailable(f"All orchestrator models unavailable: {models}. Last error: {last_error}")


def _build_grand_set_system_prompt(cards_per_set: int) -> str:
    return f"""
You are an Art Director for a mobile match-3 collectible card game,
designing the GRAND (finale) set of a collection — the celebratory high
point, shown only after the player completes the rest of the collection.

Given the grand set's theme (and optional producer notes), propose
exactly {cards_per_set} card concepts. EVERY card in this set is gold /
category 5 — this set contains ONLY gold cards, unlike regular sets.

{CATEGORY_RULES[5]}

Each card concept must show 1-2 expressive characters caught in one
small ironic or humorous emotional moment tied to the set's theme — a
lighthearted, wry twist, not a neutral or purely dramatic scene. No two
cards should depict the same joke or the same pair of characters in the
same situation.

Each card concept must also be:
- A single self-contained sentence (no readable text/writing/logos
  should ever appear on the card).
- Free of references to real branded companies, real products, or real
  named public figures.

For every card, also propose:
- "id": a short unique lowercase snake_case id, prefixed "char_" (almost
  all grand-set cards are character-driven story scenes).
- "type": almost always "character" for this set.
- "rarity": always "gold" for this set.
- "visual_category": always 5.

Return STRICT JSON only, no markdown fences, no commentary, matching
exactly this shape:

{{
  "cards": [
    {{"id": "...", "type": "character", "rarity": "gold", "visual_category": 5, "concept": "..."}}
  ]
}}
""".strip()


def generate_grand_set_concepts(
    theme: str,
    producer_notes: str = "",
    set_id: str = "set_16_grand",
    cards_per_set: Optional[int] = None,
) -> dict:
    """
    Гранд-сет (ТЗ §0.2): единственный сет коллекции, состоящий ИСКЛЮЧИТЕЛЬНО
    из золотых карточек (category 5), с кульминационной темой. В отличие
    от generate_set_concepts() (который жёстко ИСКЛЮЧАЕТ золото — ТЗ §0.3
    про тестовые/обычные сеты), здесь наоборот — жёстко ТРЕБУЕТСЯ золото
    на каждой карте, отбраковываем любую карту, которая им не является.

    Возвращает ОДИН набор (не список вариантов):
    {"set_id": "...", "cards": [10 карт, все category 5 / rarity gold]}.
    """
    base_url, api_key, models = _get_orchestrator_config()
    if not (base_url and api_key and models):
        raise OrchestratorNotConfigured(
            "LLM Orchestrator is not configured — set ORCHESTRATOR_MODEL "
            "(and optionally ORCHESTRATOR_API_BASE_URL / ORCHESTRATOR_API_KEY)."
        )

    n_cards = cards_per_set or ORCHESTRATOR_DEFAULT_CARDS_PER_SET
    system_prompt = _build_grand_set_system_prompt(n_cards)

    user_prompt = f"GRAND SET THEME: {theme}\n\n"
    if producer_notes:
        user_prompt += f"PRODUCER NOTES (free-form wishes): {producer_notes}\n\n"
    user_prompt += f"Propose exactly {n_cards} gold card concepts for this grand set, per the system prompt."

    last_error = None
    for model_index, model in enumerate(models):
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.9,
            "max_tokens": ORCHESTRATOR_MAX_TOKENS,
        }
        try:
            _throttle_orchestrator_api()
            log.info("[Collection] requesting grand set (%d gold cards) for %r (model '%s')",
                      n_cards, theme, model)

            response = _call_with_hard_timeout(
                requests.post, ORCHESTRATOR_API_TIMEOUT_SECONDS + 10,
                f"{base_url.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=ORCHESTRATOR_API_TIMEOUT_SECONDS,
            )
            if response.status_code == 429 or 400 <= response.status_code < 500:
                raise OrchestratorUnavailable(f"HTTP {response.status_code} for model '{model}' — {response.text[:500]}")
            response.raise_for_status()

            raw_text = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(_clean_json_response(raw_text))
            cards_raw = parsed.get("cards", [])

            clean_cards = []
            for card in cards_raw:
                category = card.get("visual_category")
                rarity = card.get("rarity", "gold")
                # Жёсткая защита В ОБРАТНУЮ СТОРОНУ от generate_set_concepts:
                # здесь отбраковываем всё, что НЕ золото — а не наоборот.
                if category != 5 or rarity != "gold":
                    log.warning(
                        "[Collection] dropping grand-set card '%s' — category=%r "
                        "rarity=%r is not gold/category 5",
                        card.get("id", "?"), category, rarity,
                    )
                    continue
                clean_cards.append({
                    "id": card["id"],
                    "set_id": set_id,
                    "type": card.get("type", "character"),
                    "rarity": "gold",
                    "visual_category": 5,
                    "concept": card["concept"],
                })

            if len(clean_cards) < n_cards:
                log.warning(
                    "[Collection] grand set has only %d/%d usable cards after filtering",
                    len(clean_cards), n_cards,
                )

            log.info("[Collection] grand set '%s' — %d usable gold cards", set_id, len(clean_cards))
            return {"set_id": set_id, "cards": clean_cards}

        except OrchestratorUnavailable as e:
            last_error = e
        except (requests.exceptions.RequestException, json.JSONDecodeError,
                KeyError, TypeError, ValueError) as e:
            last_error = e
            log.warning("[Collection] invalid/failed grand-set response from model '%s': %s", model, e)

        if model_index < len(models) - 1:
            log.info("[Collection] switching to fallback '%s'", models[model_index + 1])

    _log_quota_reset_if_present(str(last_error), "Collection: grand set")
    raise OrchestratorUnavailable(f"All orchestrator models unavailable: {models}. Last error: {last_error}")


def _load_collection_progress() -> dict:
    """Resume-состояние сборки коллекции — свой файл, отдельный от
    EXPORT_JSON_PATH (тот про статус картинок, этот про то, какие
    сеты текстовых концептов уже сгенерированы)."""
    if not os.path.exists(COLLECTION_EXPORT_JSON_PATH):
        return {}
    try:
        with open(COLLECTION_EXPORT_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.warning("[Collection] could not read %s (%s) — starting fresh", COLLECTION_EXPORT_JSON_PATH, e)
        return {}


def _save_collection_progress(data: dict) -> None:
    _atomic_json_write(COLLECTION_EXPORT_JSON_PATH, data)


def generate_collection(
    collection_theme: str,
    producer_notes: str = "",
    n_regular_sets: Optional[int] = None,
    resume: bool = True,
) -> dict:
    """
    Верхний уровень целиком (ТЗ §0.2): тема коллекции -> 16 сетов.

    1. Генерирует (или подхватывает из resume-файла) 15 разных подтем +
       1 кульминационную тему гранд-сета.
    2. Для каждой из 15 подтем — ОДИН вариант сета (10 карт, категории
       1-4) через generate_set_concepts(num_variants=1).
    3. Для гранд-сета — 10 золотых карт через generate_grand_set_concepts.
    4. Resume: прогресс сохраняется в COLLECTION_EXPORT_JSON_PATH после
       каждого готового сета — прерванная сборка на следующем запуске
       продолжает с недостающих сетов, не тратя LLM-вызовы повторно на
       уже готовые.

    ID карт префиксуются номером сета (set01_..., set02_..., ...,
    set16_grand_...) — гарантия уникальности id по всей коллекции, даже
    если модель в разных вызовах случайно предложит одинаковый id.

    Возвращает:
    {
        "collection_theme": "...",
        "sets": [
            {"set_number": 1, "set_id": "set01", "subtheme": "...", "cards": [...]},
            ...
            {"set_number": 15, "set_id": "set15", "subtheme": "...", "cards": [...]},
            {"set_number": 16, "set_id": "set16_grand", "subtheme": "...", "cards": [...]},  # только gold
        ],
    }
    """
    n_sets = n_regular_sets or ORCHESTRATOR_NUM_REGULAR_SETS
    progress = _load_collection_progress() if resume else {}

    if resume and progress.get("collection_theme") == collection_theme and progress.get("subthemes"):
        subthemes = progress["subthemes"]
        log.info("[Collection] resume: reusing previously generated sub-theme names")
    else:
        subthemes = generate_subtheme_names(collection_theme, producer_notes, n_sets)
        progress = {"collection_theme": collection_theme, "subthemes": subthemes, "sets": {}}
        _save_collection_progress(progress)

    regular_subthemes = subthemes["regular_subthemes"]
    grand_subtheme = subthemes["grand_subtheme"]

    # Если модель дала меньше подтем, чем просили (см. предупреждение в
    # generate_subtheme_names) — не тратим лишние LLM-вызовы, строим
    # коллекцию из того, что реально получили, а не молча зацикливаемся.
    actual_n_sets = min(n_sets, len(regular_subthemes))
    if actual_n_sets < n_sets:
        log.warning(
            "[Collection] only %d unique sub-themes available — building a %d-set "
            "collection instead of the requested %d",
            actual_n_sets, actual_n_sets, n_sets,
        )

    finished_sets = progress.setdefault("sets", {})

    for i in range(actual_n_sets):
        set_number = i + 1
        set_key = f"set{set_number:02d}"

        if resume and set_key in finished_sets:
            log.info("[Collection] resume: skipping %s (already generated)", set_key)
            continue

        subtheme = regular_subthemes[i]
        # ИСПРАВЛЕНО (найдено на реальном прогоне): сбой ОДНОГО сета
        # (например, временный upstream 429 после того, как основная И
        # fallback модели обе не ответили) раньше ронял исключение через
        # весь generate_collection() наружу — пользователь терял
        # ПЕРЕМЕННУЮ collection целиком, хотя первые N сетов уже реально
        # сгенерировались и сохранились в COLLECTION_EXPORT_JSON_PATH. По
        # аналогии с run_production_pipeline (там сбой одной карты не
        # роняет весь батч) — ловим здесь, помечаем сет как
        # незавершённый, продолжаем со следующей подтемы. Следующий
        # resume=True запуск сам подхватит именно недостающие сеты.
        try:
            variants = generate_set_concepts(
                theme=subtheme,
                producer_notes=producer_notes,
                set_id_prefix=set_key,
                num_variants=1,
                cards_per_set=ORCHESTRATOR_DEFAULT_CARDS_PER_SET,
            )
        except Exception as e:
            log.error(
                "[Collection] %s ('%s') failed — %s. Skipping for now; "
                "rerun with resume=True to retry just this set.",
                set_key, subtheme, e,
            )
            continue

        cards = variants[0]["cards"] if variants else []
        for card in cards:
            card["id"] = f"{set_key}_{card['id']}"
            card["set_id"] = set_key

        finished_sets[set_key] = {
            "set_number": set_number, "set_id": set_key,
            "subtheme": subtheme, "cards": cards,
        }
        _save_collection_progress(progress)

    grand_key = "set16_grand"
    if not (resume and grand_key in finished_sets):
        try:
            grand = generate_grand_set_concepts(
                theme=grand_subtheme, producer_notes=producer_notes, set_id=grand_key,
            )
            cards = grand["cards"]
            for card in cards:
                card["id"] = f"{grand_key}_{card['id']}"
                card["set_id"] = grand_key

            finished_sets[grand_key] = {
                "set_number": actual_n_sets + 1, "set_id": grand_key,
                "subtheme": grand_subtheme, "cards": cards,
            }
            _save_collection_progress(progress)
        except Exception as e:
            log.error(
                "[Collection] grand set ('%s') failed — %s. Skipping for now; "
                "rerun with resume=True to retry just this set.",
                grand_subtheme, e,
            )
    else:
        log.info("[Collection] resume: skipping %s (already generated)", grand_key)

    # Собираем итог ТОЛЬКО из реально готовых сетов — если что-то выше
    # было пропущено из-за сбоя, финальная сборка больше не падает с
    # KeyError, а просто возвращает то, что есть, плюс явный список
    # недостающих сетов, чтобы было видно, что докатать через resume.
    ordered_sets = []
    missing = []
    for i in range(actual_n_sets):
        key = f"set{i+1:02d}"
        if key in finished_sets:
            ordered_sets.append(finished_sets[key])
        else:
            missing.append(key)
    if grand_key in finished_sets:
        ordered_sets.append(finished_sets[grand_key])
    else:
        missing.append(grand_key)

    if missing:
        log.warning(
            "[Collection] %d set(s) missing this run: %s — rerun "
            "generate_collection(..., resume=True) to fill them in "
            "without regenerating what's already done.",
            len(missing), ", ".join(missing),
        )

    log.info(
        "[Collection] done — %d/%d set(s) ready, %d card concepts total",
        len(ordered_sets), actual_n_sets + 1, sum(len(s["cards"]) for s in ordered_sets),
    )

    return {"collection_theme": collection_theme, "sets": ordered_sets, "missing_sets": missing}


def _export_ready_assets(cards: list[dict], status_db: dict) -> list[tuple[str, str]]:
    """
    ДОБАВЛЕНО (v29, по запросу из диалога). Копирует (не переносит) файлы
    карт со статусом Ready из OUTPUT_DIR в READY_OUTPUT_DIR — отдельная
    папка только для того, что реально прошло QA, без мусора из
    Failed_QA/QA_Unavailable/промежуточных попыток, которые тоже живут в
    OUTPUT_DIR. Возвращает [(card_id, dest_path), ...] для печати в
    вызывающей функции — единообразно для render_set_variants() и
    render_collection().
    """
    os.makedirs(READY_OUTPUT_DIR, exist_ok=True)
    exported = []
    for card in cards:
        entry = status_db.get(card["id"])
        if not entry or entry.get("status") != "Ready" or not entry.get("asset_path"):
            continue
        src_filename = os.path.basename(entry["asset_path"])
        src = os.path.join(OUTPUT_DIR, src_filename)
        dst = os.path.join(READY_OUTPUT_DIR, src_filename)
        if not os.path.exists(src):
            log.warning(
                "[Export] '%s' marked Ready in status DB but file missing on disk: %s — skipping copy",
                card["id"], src,
            )
            continue
        try:
            shutil.copy2(src, dst)
            exported.append((card["id"], dst))
        except OSError as e:
            log.warning("[Export] failed to copy '%s' to %s: %s", card["id"], READY_OUTPUT_DIR, e)
    return exported


def render_collection(collection: dict, resume: bool = True) -> None:
    """
    Stage 2 для ВСЕЙ коллекции разом — тонкая обёртка над
    run_production_pipeline(): разворачивает collection["sets"] в один
    плоский список карт (id уже глобально уникальны — префиксованы
    номером сета в generate_collection) и добавляет сводку готовности
    по каждому сету в конце.

    Резюмируемость (по аналогии с основной логикой Stage 2, ТЗ §0.9) —
    уже встроена в run_production_pipeline через EXPORT_JSON_PATH: карты
    со статусом "Ready" не трогаются повторно, остальные продолжают
    сквозной счётчик попыток. Circuit breaker на MAX_CONSECUTIVE_QA_
    UNAVAILABLE не даёт впустую жечь GPU-время на карты, которые всё
    равно не пройдут QA прямо сейчас (см. run_production_pipeline).

    Поэтому этот же вызов безопасно повторять много раз подряд — при
    каждом перезапуске коллекция естественным образом дозаполняется тем,
    что не успело/не смогло сгенерироваться в прошлый раз, а не
    начинает всё с нуля.
    """
    # ИСПРАВЛЕНО (ревью): если generate_collection() закончился с
    # collection["missing_sets"] непустым (какие-то сеты не сгенерировали
    # ТЕКСТ вообще — LLM упал), render_collection раньше молча рендерил
    # только то, что есть в collection["sets"], и нигде не говорил, что
    # часть коллекции отсутствует ещё на уровне концептов, а не картинок.
    if collection.get("missing_sets"):
        print("⚠ ВНИМАНИЕ: следующие сеты не были сгенерированы на этапе концептов "
              "(Stage 1) и не попадут в рендер вообще:")
        print("   " + ", ".join(collection["missing_sets"]))
        print("   Запустите generate_collection(..., resume=True) повторно, чтобы их добрать.")
        print()

    all_cards = [card for s in collection["sets"] for card in s["cards"]]
    log.info(
        "[Collection] rendering %d card(s) across %d set(s)",
        len(all_cards), len(collection["sets"]),
    )
    run_production_pipeline(all_cards, resume=resume)

    status_db = _load_status_database()
    print("=" * 90)
    print("СВОДКА ГОТОВНОСТИ ПО СЕТАМ")
    print("=" * 90)
    total_ready = 0
    total_cards = 0
    for s in collection["sets"]:
        ready = sum(1 for c in s["cards"] if status_db.get(c["id"], {}).get("status") == "Ready")
        total_ready += ready
        # ИСПРАВЛЕНО (ревью): сет с <10 концептами (LLM не досдал карты —
        # generate_set_concepts уже логирует warning, но в этой сводке это
        # раньше было не видно вообще) помечается отдельно, чтобы не
        # спутать с "ещё не отрендерено".
        if len(s["cards"]) < ORCHESTRATOR_DEFAULT_CARDS_PER_SET:
            print(f"⚠ Сет {s['set_number']:2d} ({s['set_id']}) содержит только "
                  f"{len(s['cards'])}/{ORCHESTRATOR_DEFAULT_CARDS_PER_SET} концептов "
                  f"(LLM недосдал карты на этапе Stage 1)")
        total_cards += len(s["cards"])
        marker = "✅" if ready == len(s["cards"]) else ("⏳" if ready > 0 else "❌")
        print(f"{marker} Сет {s['set_number']:2d} ({s['set_id']:>12}) — {s['subtheme']:<30} — {ready}/{len(s['cards'])} Ready")
    print("-" * 90)
    print(f"ИТОГО: {total_ready}/{total_cards} карт готовы")

    exported = _export_ready_assets(all_cards, status_db)
    print()
    print(f"Готовые файлы скопированы в {READY_OUTPUT_DIR}/ ({len(exported)} шт.):")
    for card_id, dst in exported:
        print(f"  [{card_id}] {dst}")


# ============================================================
# 22.6 ОДИН СЕТ С ВАРИАНТАМИ — Stage 1 + Stage 2 в одном вызове
# ============================================================
# В отличие от generate_collection()/render_collection() (весь 16-сетовый
# гранд-объём), это — путь для сдачи по буквальному ТЗ: один сет,
# 3-5 вариантов на выбор продюсера, без золота. generate_set_concepts()
# уже делает всю нужную Stage-1 работу (тема -> N вариантов по 10 карт,
# категории 1-4, золото жёстко отбраковано на уровне оркестратора) —
# этой обёртки не хватало только на уровне "прогнать всё одним вызовом
# и посчитать сводку по вариантам", как render_collection() делает для
# полной коллекции.

def render_set_variants(
    theme: str,
    producer_notes: str = "",
    set_id_prefix: str = "set",
    num_variants: Optional[int] = None,
    cards_per_set: Optional[int] = None,
    resume: bool = True,
) -> list[dict]:
    """
    Stage 1: generate_set_concepts(theme, ...) -> num_variants вариантов
    по cards_per_set карт (категории 1-4, без золота). ИСПРАВЛЕНО (v32):
    Stage 1 теперь тоже резюмируем (SET_VARIANTS_EXPORT_JSON_PATH) — 
    повторный вызов с той же темой не генерирует всё заново, а
    переиспользует уже сгенерированные варианты и достраивает только
    недостающие (см. docstring generate_set_concepts).
    Stage 2: разворачивает ВСЕ варианты в один плоский список и рендерит
    все сразу через run_production_pipeline (resume работает как обычно,
    по card.id, id уже включает "_v{n}" на уровне set_id — коллизий
    между вариантами нет).

    Возвращает variants (тот же формат, что и generate_set_concepts) —
    после рендера variants[i]["cards"][j]["id"] можно смотреть в
    EXPORT_JSON_PATH для статуса/пути к готовой картинке каждой карты.
    """
    variants = generate_set_concepts(
        theme=theme,
        producer_notes=producer_notes,
        set_id_prefix=set_id_prefix,
        num_variants=num_variants,
        cards_per_set=cards_per_set,
        resume=resume,
    )

    n_cards_expected = cards_per_set or ORCHESTRATOR_DEFAULT_CARDS_PER_SET
    required_categories = {1, 2, 3, 4}

    print("=" * 90)
    print(f"ТЕМА: {theme!r} — {len(variants)} вариант(ов) сгенерировано")
    print("=" * 90)
    for v in variants:
        categories_present = {c["visual_category"] for c in v["cards"]}
        missing = required_categories - categories_present
        gold_leak = [c["id"] for c in v["cards"] if c.get("rarity") == "gold"]
        if missing:
            status = f"⚠ НЕ ХВАТАЕТ категорий: {sorted(missing)}"
        elif gold_leak:
            status = f"⚠ ОШИБКА — золото просочилось: {gold_leak}"
        elif len(v["cards"]) < n_cards_expected:
            status = f"⚠ неполный вариант: {len(v['cards'])}/{n_cards_expected} карт"
        else:
            status = "OK — все 4 категории на месте, без золота"
        print(f"[{v['set_id']}] {v['variant_label']} — {len(v['cards'])} карт(ы) — {status}")
        for c in v["cards"]:
            print(f"  [{c['id']:<32}] cat={c['visual_category']}  {c['rarity']:<7}  {c['type']:<10} — {c['concept']}")
    print("=" * 90)

    all_cards = [card for v in variants for card in v["cards"]]
    log.info(
        "[SetVariants] rendering %d card(s) across %d variant(s) of theme %r",
        len(all_cards), len(variants), theme,
    )
    run_production_pipeline(all_cards, resume=resume)

    status_db = _load_status_database()
    print("=" * 90)
    print("СВОДКА ГОТОВНОСТИ ПО ВАРИАНТАМ")
    print("=" * 90)
    total_ready = 0
    total_cards = 0
    for v in variants:
        ready = sum(1 for c in v["cards"] if status_db.get(c["id"], {}).get("status") == "Ready")
        total_ready += ready
        total_cards += len(v["cards"])
        marker = "✅" if ready == len(v["cards"]) else ("⏳" if ready > 0 else "❌")
        print(f"{marker} [{v['set_id']}] {v['variant_label']:<30} — {ready}/{len(v['cards'])} Ready")
    print("-" * 90)
    print(f"ИТОГО: {total_ready}/{total_cards} карт готовы")

    exported = _export_ready_assets(all_cards, status_db)
    print()
    print(f"Готовые файлы скопированы в {READY_OUTPUT_DIR}/ ({len(exported)} шт.):")
    for card_id, dst in exported:
        print(f"  [{card_id}] {dst}")

    return variants


# ============================================================
# 23. RUN
# ============================================================

print(f"[card_pipeline.py] loaded — PIPELINE_FILE_VERSION={PIPELINE_FILE_VERSION}")

# ИСПРАВЛЕНО: "if __name__ == '__main__':" здесь НЕ защита, а лишняя
# иллюзия защиты — в Jupyter/Kaggle-ячейке __name__ тоже "__main__",
# поэтому run_production_pipeline() запускался при КАЖДОМ выполнении
# этой ячейки (например, просто чтобы получить обновлённые определения
# функций перед diagnose_prompt_ablation), без явного намерения запускать
# прод-пайплайн — и без спроса тратил дневную QA-квоту.
#
# Теперь запуск — явный opt-in через переменную окружения, а не побочный
# эффект загрузки ячейки. По умолчанию выключено. Хочешь прогнать
# пайплайн — либо поставь os.environ["AUTO_RUN_PIPELINE"] = "true" в
# конфиг-ячейке, либо (проще и нагляднее) вызови
# run_production_pipeline(validated_cards) явно в отдельной ячейке —
# ablation/preview от этого никак не зависят и не запускают его сами.
if os.environ.get("AUTO_RUN_PIPELINE", "false").strip().lower() == "true":
    run_production_pipeline(validated_cards)
else:
    print("[card_pipeline.py] AUTO_RUN_PIPELINE is off — pipeline NOT started automatically. "
          "Call run_production_pipeline(validated_cards) explicitly when ready.")
