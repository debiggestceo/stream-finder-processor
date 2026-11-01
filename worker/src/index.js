const PROCESSOR_URL = "https://stream-finder-processor.onrender.com";
const GROQ_CHAT_URL = "https://api.groq.com/openai/v1/chat/completions";
const GROQ_CHAT_MODEL = "openai/gpt-oss-120b";

const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: "📊 Статус" }, { text: "❌ Отменить" }],
    [{ text: "❓ Как пользоваться" }]
  ],
  resize_keyboard: true,
  is_persistent: true
};

async function telegram(env, chatId, text, withKeyboard = false) {
  const message = String(text).slice(0, 4000);
  return fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      text: message,
      ...(withKeyboard ? { reply_markup: MAIN_KEYBOARD } : {})
    })
  });
}

function detectPlatform(url) {
  try {
    const host = new URL(url).hostname.toLowerCase().replace(/^www\./, "");
    if (host === "kick.com") return "kick";
    if (host === "twitch.tv" || host.endsWith(".twitch.tv")) return "twitch";
    if (host === "youtube.com" || host.endsWith(".youtube.com") || host === "youtu.be") return "youtube";
  } catch {}
  return "unknown";
}

function extractStreamUrl(text) {
  const match = String(text).match(/https?:\/\/[^\s]+/i);
  if (!match) return null;
  const url = match[0].replace(/[),.!?]+$/, "");
  return ["kick", "twitch", "youtube"].includes(detectPlatform(url)) ? url : null;
}

async function cancelProcessorJob(env, jobId) {
  try {
    await fetch(PROCESSOR_URL + "/cancel-job", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Processor-Secret": env.PROCESSOR_SECRET },
      body: JSON.stringify({ job_id: jobId })
    });
  } catch (error) { console.error("Processor cancel request failed:", error); }
}

async function dispatchChunk(env, job) {
  await env.DB.prepare(`
    UPDATE stream_job_state SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE job_id = ?
  `).bind(job.job_id).run();

  const response = await fetch(`${PROCESSOR_URL}/process-chunk`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Processor-Secret": env.PROCESSOR_SECRET },
    body: JSON.stringify({
      url: job.url, chat_id: job.chat_id, job_id: job.job_id, chunk_index: job.next_chunk,
      platform: job.platform || detectPlatform(job.url)
    })
  });

  if (!response.ok) {
    const body = await response.text();
    await env.DB.prepare(`
      UPDATE stream_job_state SET status = 'processing', updated_at = datetime('now', '-11 minutes'), error = ? WHERE job_id = ?
    `).bind(body.slice(0, 1000), job.job_id).run();
    throw new Error(`Render error ${response.status}: ${body}`);
  }
}

async function startJob(env, chatId, url) {
  const active = await env.DB.prepare(`
    SELECT job_id, next_chunk FROM stream_job_state
    WHERE chat_id = ? AND status IN ('processing', 'running')
    ORDER BY updated_at DESC LIMIT 1
  `).bind(String(chatId)).first();

  if (active) {
    const minute = Number(active.next_chunk || 0) * 10;
    await telegram(env, chatId, `У тебя уже обрабатывается одна запись. Сейчас примерно до ${minute}-й минуты. Дождись завершения или отправь /cancel, чтобы отменить её.`);
    return { status: "already_processing", job_id: active.job_id };
  }

  const recent = await env.DB.prepare(`
    SELECT updated_at FROM stream_job_state
    WHERE chat_id = ? ORDER BY updated_at DESC LIMIT 1
  `).bind(String(chatId)).first();

  if (recent?.updated_at) {
    const lastMs = Date.parse(String(recent.updated_at).replace(" ", "T") + "Z");
    const cooldownMs = 60 * 1000;
    if (Number.isFinite(lastMs) && Date.now() - lastMs < cooldownMs) {
      const left = Math.ceil((cooldownMs - (Date.now() - lastMs)) / 1000);
      await telegram(env, chatId, `Подожди ${left} сек. перед запуском следующего видео.`);
      return { status: "cooldown" };
    }
  }

  const jobId = crypto.randomUUID();
  const platform = detectPlatform(url);

  await env.DB.prepare(`
    INSERT INTO streams (chat_id, url, platform, status, job_id)
    VALUES (?, ?, ?, 'processing', ?)
  `).bind(String(chatId), url, platform, jobId).run();

  await env.DB.prepare(`
    INSERT INTO stream_job_state (job_id, chat_id, url, next_chunk, status)
    VALUES (?, ?, ?, 0, 'processing')
  `).bind(jobId, String(chatId), url).run();

  const job = { job_id: jobId, chat_id: String(chatId), url, platform, next_chunk: 0 };
  await dispatchChunk(env, job);

  await telegram(env, chatId, "🎬 Принял запись! Начинаю обработку. Сообщу, когда всё будет готово. Статус можно посмотреть кнопкой ниже.", true);
}

async function processorChunk(request, env) {
  if (!env.PROCESSOR_SECRET || request.headers.get("X-Processor-Secret") !== env.PROCESSOR_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }

  const data = await request.json();
  const jobId = String(data.job_id || "");
  const chatId = String(data.chat_id || "");
  const chunkIndex = Number(data.chunk_index);
  const status = data.status;

  if (!jobId || !chatId || !Number.isInteger(chunkIndex) || chunkIndex < 0) {
    return Response.json({ error: "Invalid callback" }, { status: 400 });
  }

  const job = await env.DB.prepare(`SELECT * FROM stream_job_state WHERE job_id = ?`).bind(jobId).first();
  if (!job) return Response.json({ error: "Job not found" }, { status: 404 });

  if (job.status === "cancelled") {
    return Response.json({ status: "cancelled", job_id: jobId });
  }

  if (chunkIndex !== job.next_chunk) {
    return Response.json({ status: "ignored", expected_chunk: job.next_chunk });
  }

  if (status === "error") {
    const error = String(data.error || "Unknown processing error");
    await env.DB.prepare(`
      UPDATE stream_job_state SET status = 'processing', updated_at = datetime('now', '-11 minutes'), error = ? WHERE job_id = ?
    `).bind(error.slice(0, 1000), jobId).run();

    await telegram(env, chatId, `Ошибка при обработке фрагмента ${chunkIndex + 1}. Попробую повторить.`);
    return Response.json({ status: "retry_scheduled" });
  }

  if (status === "completed") {
    await env.DB.prepare(`
      UPDATE stream_job_state SET status = 'completed', updated_at = CURRENT_TIMESTAMP, error = NULL WHERE job_id = ?
    `).bind(jobId).run();

    const rows = await env.DB.prepare(`
      SELECT start_time, end_time, transcript FROM stream_segments WHERE job_id = ? ORDER BY segment_index
    `).bind(jobId).all();

    const segments = (rows.results || []).map(row => ({ time: row.start_time, end_time: row.end_time, text: row.transcript }));
    const transcript = segments.map(s => `[${s.time}] ${s.text}`).join("\n");

    await env.DB.prepare(`
      UPDATE streams SET status = 'completed', transcript = ?, error = NULL WHERE job_id = ?
    `).bind(transcript, jobId).run();

    await telegram(env, chatId, "✅ Готово! Обработана вся запись.\n\nТеперь можешь задавать вопросы по стриму.", true);
    return Response.json({ status: "completed" });
  }

  if (status !== "chunk_completed") return Response.json({ error: "Unknown status" }, { status: 400 });

  const segments = Array.isArray(data.segments) ? data.segments : [];
  const statements = segments.map((segment, i) => env.DB.prepare(`
    INSERT OR IGNORE INTO stream_segments (job_id, chat_id, segment_index, start_time, end_time, transcript)
    VALUES (?, ?, ?, ?, ?, ?)
  `).bind(
    jobId, chatId, chunkIndex * 10000 + i,
    String(segment.time || "00:00:00"),
    formatTime(Number(segment.end || segment.start || 0)),
    String(segment.text || "")
  ));
  if (statements.length) await env.DB.batch(statements);

  await env.DB.prepare(`
    UPDATE stream_job_state SET next_chunk = ?, status = 'processing', updated_at = CURRENT_TIMESTAMP, error = NULL WHERE job_id = ?
  `).bind(chunkIndex + 1, jobId).run();

  try {
    await dispatchChunk(env, { job_id: jobId, chat_id: chatId, url: job.url, platform: detectPlatform(job.url), next_chunk: chunkIndex + 1 });
  } catch (error) {
    console.error("Next chunk dispatch failed:", error);
  }

  return Response.json({ status: "saved", saved_segments: segments.length, next_chunk: chunkIndex + 1 });
}

function formatTime(seconds) {
  seconds = Math.max(0, Math.floor(seconds));
  return [Math.floor(seconds / 3600), Math.floor((seconds % 3600) / 60), seconds % 60]
    .map(v => String(v).padStart(2, "0")).join(":");
}

function normalizeText(text) {
  return String(text || "").toLowerCase().replace(/ё/g, "е").replace(/[^\p{L}\p{N}\s]/gu, " ").replace(/\s+/g, " ").trim();
}

const STOP_WORDS = new Set(["а","и","но","или","в","на","по","с","со","к","у","из","за","от","до","не","что","это","он","она","они","его","ее","их","мы","вы","я","ты","ли","бы","же","ну","вот","там","тут","как","про","мне","меня","него","нее","the","and","for","you","that","this","with"]);

async function expandSearchTerms(env, question) {
  if (!env.GROQ_API_KEY) return [question];
  try {
    const response = await fetch(GROQ_CHAT_URL, {
      method: "POST",
      headers: { "Authorization": `Bearer ${env.GROQ_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: GROQ_CHAT_MODEL, temperature: 0,
        messages: [
          { role: "system", content: "Верни только JSON-массив коротких поисковых фраз на русском языке. Добавь разговорные варианты и синонимы. Максимум 12 элементов." },
          { role: "user", content: question }
        ]
      })
    });
    if (!response.ok) return [question];
    const result = await response.json();
    const match = (result.choices?.[0]?.message?.content || "").match(/\[[\s\S]*\]/);
    if (!match) return [question];
    const terms = JSON.parse(match[0]);
    return [question, ...terms].filter(x => typeof x === "string").slice(0, 13);
  } catch (error) {
    console.error("Search expansion failed:", error);
    return [question];
  }
}

function timeToSeconds(value) {
  const parts = String(value || "00:00:00").split(":").map(Number);
  if (parts.length !== 3 || parts.some(n => !Number.isFinite(n))) return 0;
  return parts[0] * 3600 + parts[1] * 60 + parts[2];
}

function detectTimeScope(question, segments) {
  const q = normalizeText(question);
  const last = segments.reduce((max, s) => Math.max(max, timeToSeconds(s.end_time || s.start_time)), 0);
  if (/(начал|с самого начала|первые минуты|первую минут|в начале|начало видео|начало стрима)/.test(q)) {
    return { label: "начало записи", from: 0, to: Math.min(180, last || 180) };
  }
  if (/(конц|под конец|в конце|последние минуты|последнюю минут)/.test(q)) {
    return { label: "конец записи", from: Math.max(0, last - 180), to: last || 180 };
  }
  if (/(середин|в середине|примерно посередине)/.test(q)) {
    const center = last / 2;
    return { label: "середина записи", from: Math.max(0, center - 90), to: Math.min(last, center + 90) };
  }
  return null;
}

async function handleQuestion(env, chatId, question) {
  const stream = await env.DB.prepare(`
    SELECT job_id, status FROM streams WHERE chat_id = ? ORDER BY id DESC LIMIT 1
  `).bind(String(chatId)).first();

  if (!stream) return telegram(env, chatId, "Сначала отправь ссылку на запись Kick, Twitch или YouTube.");
  if (stream.status !== "completed") return telegram(env, chatId, "Запись ещё обрабатывается. Задай вопрос после завершения.");

  const rows = await env.DB.prepare(`
    SELECT start_time, end_time, transcript FROM stream_segments WHERE job_id = ? ORDER BY segment_index
  `).bind(stream.job_id).all();

  const segments = rows.results || [];
  if (!segments.length) return telegram(env, chatId, "В этой записи пока нет сохранённой расшифровки.");

  const scope = detectTimeScope(question, segments);
  let selected = [];

  if (scope) {
    selected = segments.filter(s => {
      const t = timeToSeconds(s.start_time);
      return t >= scope.from && t <= scope.to;
    }).slice(0, 60);
  } else {
    const terms = await expandSearchTerms(env, question);
    const normalizedTerms = terms.map(normalizeText).filter(Boolean);
    const words = normalizeText(question).split(" ").filter(w => w.length > 2 && !STOP_WORDS.has(w));

    const questionAnchors = [...new Set(
      normalizeText(question)
        .split(" ")
        .filter(w => w.length >= 5 && !STOP_WORDS.has(w))
    )];

    const ranked = segments.map((segment, index) => {
      const text = normalizeText(segment.transcript);
      let score = 0;
      for (const term of normalizedTerms) {
        const normalized = normalizeText(term);
        if (normalized.length > 2 && text.includes(normalized)) score += 3;
      }
      for (const word of words) if (text.includes(word)) score += 1;

      const matchedAnchors = questionAnchors.filter(anchor => text.includes(anchor));
      return { ...segment, index, score, matchedAnchors };
    }).filter(s => s.score > 0).sort((a,b) => b.score - a.score).slice(0, 12);

    // Имя может не произноситься вслух. Поэтому НЕ требуем буквального совпадения
    // имени: expandSearchTerms уже добавляет варианты, прозвища и связанные поисковые
    // фразы. Но модели явно запрещено считать совпадение доказательством, пока она не
    // увидит во фрагменте узнаваемый контент.
    const indexes = new Set();
    for (const hit of ranked) {
      for (let i = Math.max(0, hit.index - 2); i <= Math.min(segments.length - 1, hit.index + 2); i++) indexes.add(i);
    }
    selected = [...indexes].sort((a,b) => a-b).slice(0, 45).map(i => segments[i]);
  }

  if (!selected.length) {
    return telegram(env, chatId, scope
      ? `В ${scope.label} не нашлось распознанной речи.`
      : "Не нашёл подходящих фрагментов в расшифровке. Подтвердить это по тексту не могу.");
  }

  const context = selected.map(s => {
    const text = String(s.transcript || "").trim();
    return `[${s.start_time}] ${text}`;
  }).join("\n");

  if (!env.GROQ_API_KEY) {
    return telegram(env, chatId, selected.slice(0, 10).map(s => `[${s.start_time}] ${s.transcript}`).join("\\n"));
  }

  try {
    const response = await fetch(GROQ_CHAT_URL, {
      method: "POST",
      headers: { "Authorization": `Bearer ${env.GROQ_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: GROQ_CHAT_MODEL, temperature: 0,
        messages: [
          { role: "system", content: "Ты анализируешь расшифровку Whisper и компактные визуальные метки из видео. Метки начинаются с «[ВИДЕО-МЕТКА]» и означают, что система увидела конкретный признак на экране около этого таймкода. Расшифровка может ошибаться даже в грамматически гладких фразах. Не утверждай, что слова точно произнесены. Отвечай по-русски, ясно и без воды.\n\nГЛАВНЫЙ ПРИНЦИП: лучше дать короткий и честный ответ, чем длинный ответ из догадок. Не пытайся заполнить каждый таймкод интерпретацией. Расшифровка — ненадёжный источник; не достраивай обрывки.\n\nСТРОГАЯ КЛАССИФИКАЦИЯ КАЖДОГО ФРАГМЕНТА:\n1) «Ясно распознано в тексте» — только если мысль понятна сама по себе, без восстановления пропущенных слов. Это оценка читаемости текста, не подтверждение аудио.\n2) «Вероятно» — используй редко и только если фрагмент содержит связную, грамматически достаточно понятную мысль, но один конкретный элемент остаётся сомнительным. Обязательно назови, что именно неясно.\n3) «Не удалось разобрать» — если это набор слов, обрывок, странная грамматика, бессмысленное сочетание или для понимания приходится угадывать, кто что сделал и о чём речь.\n\nПРАВИЛО ВЗАИМОИСКЛЮЧЕНИЯ: каждый временной фрагмент относится только к ОДНОМУ разделу. Если фраза неразборчива, укажи её только в «Не удалось разобрать» — никогда одновременно в «Вероятно». Если у фрагмента есть и понятная часть, и отдельный бессвязный хвост, раздели их по времени только когда границы реально видны; иначе классифицируй весь фрагмент как неразборчивый. Не повторяй один таймкод или перекрывающийся диапазон в разных разделах.\n\nВАЖНО: отдельные слова, ругань, междометия, обрывки вроде «уже будем», «всё-таки какой-то», «вот я», «и интернетом, и вы», а также странные сочетания вроде «мала мать плакал» НЕ являются основанием для раздела «Вероятно». Не пытайся объяснять их по словам. Переноси их в «Не удалось разобрать» или пропускай, если они не важны для вопроса.\nЕсли в фрагменте распознаются только отдельные слова («мигрант», «азиатки», «лагеря», название игры), сообщай только, что эти слова встречаются; не делай из них вывод о теме, людях или событиях.\nНе превращай вопросительную фразу в установленное событие: «Меня забанили или что?» означает только, что человек задаёт такой вопрос, а не что его действительно забанили.\n\nОБЩИЙ СМЫСЛ:\n- Пиши только темы, которые поддерживаются несколькими понятными фрагментами или одной совершенно ясной законченной репликой.\n- Не склеивай несвязные участки в причинно-следственную историю.\n- Если между двумя понятными темами большой неразборчивый промежуток, прямо обозначь разрыв.\n- Не называй рассуждение длительным и не приписывай аргументы, которых нет в понятных фрагментах.\n- Если начало записи почти полностью неразборчиво, так и скажи; не перечисляй догадки о мигрантах, азиатках и прочем как содержание монолога.\n\nЦИТАТЫ:\n- Дословно цитируй только короткую, понятную фразу, целиком присутствующую в одном фрагменте.\n- Не склеивай строки, не исправляй слова, не дополняй начало или конец цитаты.\n- Не ставь пересказ в кавычки.\n- Не цитируй одиночную ругань и бессодержательные обрывки — они не помогают ответить на вопрос.\n- Если фраза понятна лишь после исправления Whisper, не называй её дословной цитатой.\n\nКОНТЕКСТ И НАЗВАНИЯ:\n- Не угадывай пропущенные слова, имена, события, мотивы или адресата.\n- Имя человека или исполнителя может не произноситься вслух. В таком случае можно подтвердить его только по действительно узнаваемому содержанию: названию произведения, характерной цитате, тексту песни или другому конкретному признаку, который есть во фрагменте. Нельзя считать доказательством просто слова «слушал», «смотрел» или «играл».
- Если имя не произнесено, но контент позволяет достаточно уверенно идентифицировать автора или исполнителя, сначала кратко объясни, какой именно признак это подтверждает, и только потом дай ответ. Если такого признака нет — честно скажи, что подтвердить нельзя.\n- Название игры/человека нормализуй только при однозначной опоре на несколько независимых подсказок. При сомнении укажи, что распознавание искажено.\n- Узнаваемое название не доказывает точность всей реплики вокруг него.\n- Не приписывай говорящему позицию, намерение или оскорбление, если это не выражено понятным фрагментом.\n\nТАЙМКОДЫ:\n- Для вопросов о начале, конце и середине анализируй весь переданный интервал по порядку.\n- Таймкод ставь только у конкретного фрагмента, который действительно находится рядом с ним.\n- Копируй таймкод ТОЧНО из входной строки в квадратных скобках. Не меняй цифры, не добавляй/убирай часы и не преобразуй 00:01:14 в 01:01:14.\n- Не используй таймкод, которого нет среди входных строк; для диапазона используй только реальные начальную и конечную отметки входных фрагментов.\n- Не придумывай точность до секунды и не объединяй разные временные участки.\n- Если мысль обрывается, не сочиняй продолжение. Большие неразборчивые промежутки обозначай диапазоном, если его границы видны.\n\nФОРМАТ:\nСначала «Общий смысл» — краткий и осторожный пересказ по порядку.\nЗатем при наличии:\n- «Ясно распознано в тексте» — только действительно понятные самостоятельные мысли, каждая отдельно с таймкодом.\n- «Вероятно» — максимум несколько осторожных трактовок; если связного смысла нет, этот раздел вообще не нужен.\n- «Не удалось разобрать» — проблемные интервалы и короткие причины, без попыток перевести бессмыслицу в осмысленную речь.\nНе дублируй одну мысль в разных разделах. Не нужно перечислять каждую ошибочную строку: группируй соседние неразборчивые фрагменты в интервалы. Короткий ответ предпочтительнее длинного.\n\nФИНАЛЬНАЯ ПРОВЕРКА:\n- Не принял ли я вопрос за событие?\n- Не превратил ли одно распознанное слово в целую тему?\n- Не объяснил ли я бессвязный обрывок через фантазию?\n- Не назвал ли я пересказ цитатой?\n- Не добавил ли я в общий смысл деталей, которые есть только в разделе «Вероятно»?\n- Нет ли одного и того же фрагмента сразу в двух разделах?\n- Каждый ли выведенный таймкод дословно присутствует во входных строках?\nЕсли хоть на один вопрос ответ «да» — перепиши ответ короче и осторожнее." },
          { role: "user", content: `Вопрос: ${question}${scope ? `\nВременной участок: ${scope.label} (${formatTime(scope.from)}–${formatTime(scope.to)}). Проанализируй его целиком.\n` : ""}\nФрагменты расшифровки по порядку:\n${context}` }
        ]
      })
    });
    if (!response.ok) throw new Error(await response.text());
    const result = await response.json();
    return telegram(env, chatId, result.choices?.[0]?.message?.content || "Не удалось сформировать ответ.");
  } catch (error) {
    console.error("Groq answer error:", error);
    return telegram(env, chatId, "Не удалось обработать вопрос. Попробуй ещё раз.");
  }
}

async function showStatus(env, chatId) {
  const job = await env.DB.prepare(`
    SELECT job_id, next_chunk, status, updated_at
    FROM stream_job_state
    WHERE chat_id = ?
    ORDER BY updated_at DESC LIMIT 1
  `).bind(String(chatId)).first();

  if (!job) return telegram(env, chatId, "Пока нет ни одной обработки. Просто отправь ссылку на запись.", true);

  if (job.status === "completed") {
    return telegram(env, chatId, "✅ Последняя запись полностью обработана. Можешь задавать вопросы по стриму.", true);
  }

  if (job.status === "cancelled") {
    return telegram(env, chatId, "❌ Последняя обработка была отменена. Можешь отправить новую ссылку.", true);
  }

  if (job.status === "processing" || job.status === "running") {
    const minute = Number(job.next_chunk || 0) * 10;
    return telegram(env, chatId, `⏳ Идёт обработка. Сейчас обработано примерно до ${minute}-й минуты. Второе видео пока отправлять не нужно.`, true);
  }

  return telegram(env, chatId, `Статус последней обработки: ${job.status}`, true);
}

async function cancelJob(env, chatId) {
  const job = await env.DB.prepare(`
    SELECT job_id, next_chunk FROM stream_job_state
    WHERE chat_id = ? AND status IN ('processing', 'running')
    ORDER BY updated_at DESC LIMIT 1
  `).bind(String(chatId)).first();

  if (!job) return telegram(env, chatId, "Сейчас у тебя нет активной обработки.", true);

  await env.DB.prepare(`
    UPDATE stream_job_state SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP, error = 'Cancelled by user'
    WHERE job_id = ?
  `).bind(job.job_id).run();

  await env.DB.prepare(`
    UPDATE streams SET status = 'cancelled', error = 'Cancelled by user'
    WHERE job_id = ?
  `).bind(job.job_id).run();

  await cancelProcessorJob(env, job.job_id);
  return telegram(env, chatId, `❌ Обработку отменил. Остановился примерно на ${Number(job.next_chunk || 0) * 10}-й минуте.`, true);
}

async function handleTelegram(update, env) {
  const message = update.message;
  if (!message?.text || !message.chat?.id) return;

  const chatId = message.chat.id;
  const text = message.text.trim();

  if (text === "/cancel" || text === "❌ Отменить") return cancelJob(env, chatId);
  if (text === "📊 Статус") return showStatus(env, chatId);

  if (text === "❓ Как пользоваться") {
    return telegram(env, chatId,
      "1. Отправь ссылку на запись Kick, Twitch или YouTube.\n2. Дождись обработки.\n3. Задавай любые вопросы по содержанию.\n\nНапример: «Он слушал Понасенкова?» или «О чём говорили в начале?»",
      true
    );
  }

  if (text === "/start" || text === "/help") {
    return telegram(env, chatId,
      "Привет! Я Stream Finder.\n\nОтправь ссылку на запись Kick, Twitch или YouTube — я проанализирую стрим, а потом сможешь задавать вопросы по нему.\n\nКнопки снизу помогут посмотреть статус или отменить обработку.",
      true
    );
  }

  const streamUrl = extractStreamUrl(text);
  if (streamUrl) {
    try {
      await startJob(env, chatId, streamUrl);
    } catch (error) {
      console.error("Start job error:", error);
      await telegram(env, chatId, "Не удалось запустить обработку: " + String(error).slice(0, 500));
    }
    return;
  }

  if (text.startsWith("/")) return telegram(env, chatId, "Неизвестная команда. Используй /start.");
  return handleQuestion(env, chatId, text);
}

async function recoverJobs(env) {
  const result = await env.DB.prepare(`
    SELECT job_id, chat_id, url, next_chunk FROM stream_job_state
    WHERE status IN ('processing', 'running') AND updated_at < datetime('now', '-10 minutes')
    LIMIT 5
  `).all();

  for (const job of result.results || []) {
    try { await dispatchChunk(env, { ...job, platform: detectPlatform(job.url) }); }
    catch (error) { console.error("Recovery failed:", job.job_id, error); }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return Response.json({ status: "Stream Finder Worker online" });
    }

    if (request.method === "POST" && url.pathname === "/processor-chunk") {
      try { return await processorChunk(request, env); }
      catch (error) {
        console.error("Processor callback error:", error);
        return Response.json({ error: String(error) }, { status: 500 });
      }
    }

    if (request.method === "POST") {
      try {
        await handleTelegram(await request.json(), env);
        return Response.json({ ok: true });
      } catch (error) {
        console.error("Telegram handler error:", error);
        return Response.json({ ok: false }, { status: 500 });
      }
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(recoverJobs(env));
  }
};
