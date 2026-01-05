const state = {
  matchId: null,
  matchKey: null,
  matchCode: null,
  holeIndex: 0,
  hole: null,
  holes: [],
  players: [],
  selectedPlayerId: null,
  scores: {},
  originalScores: {},
};

const overlay = document.getElementById("codeOverlay");
const codeForm = document.getElementById("codeForm");
const codeInput = document.getElementById("matchCodeInput");
const codeSubmitBtn = document.getElementById("matchCodeSubmit");
const codeError = document.getElementById("codeError");

function showCodeError(message) {
  if (codeError) {
    codeError.textContent = message || "";
  }
}

function hideCodeOverlay() {
  overlay?.classList.add("hidden");
}

function renderHole() {
  const hole = state.hole;
  const holeText = document.getElementById("holeText");
  const holeMeta = document.getElementById("holeMeta");
  if (!hole) {
    holeText.textContent = "Hole —";
    holeMeta.textContent = "Par —";
    return;
  }
  holeText.textContent = `Hole ${hole.number}`;
  const hcpLabel = hole.handicap !== undefined ? hole.handicap : "—";
  holeMeta.textContent = `Par ${hole.par} • HCP ${hcpLabel}`;
  updateHoleNavLabels();
}

function updateHoleNavLabels() {
  const prev = document.getElementById("prevHole");
  const next = document.getElementById("nextHole");
  if (!state.holes.length) {
    prev.textContent = "< Hole —";
    next.textContent = "Hole — >";
    prev?.classList.add("disabled");
    next?.classList.add("disabled");
    return;
  }
  const prevHole = state.holes[state.holeIndex - 1];
  const nextHole = state.holes[state.holeIndex + 1];
  if (prev) {
    prev.textContent = prevHole ? `< Hole ${prevHole.hole_number}` : "";
    prev.classList.toggle("disabled", !prevHole);
  }
  if (next) {
    next.textContent = nextHole ? `Hole ${nextHole.hole_number} >` : "";
    next.classList.toggle("disabled", !nextHole);
  }
}

function makeScoreRow(player) {
  const row = document.createElement("div");
  row.className = "score-row";
  row.dataset.playerId = player.id;

  const value = state.scores[player.id] ?? "";
  row.innerHTML = `
    <div>
      <div class="name">${player.name}</div>
      <div class="hcp">(${player.handicap})</div>
    </div>
    <div style="display:flex; align-items:center;">
      <div class="value" aria-label="Score entry">${value || ""}</div>
      <div class="dot" aria-hidden="true"></div>
    </div>
  `;

  row.addEventListener("click", () => {
    state.selectedPlayerId = player.id;
    renderScores();
  });

  return row;
}

function renderScores() {
  const list = document.getElementById("scoreList");
  if (!list) return;
  list.innerHTML = "";
  if (!state.players.length) {
    list.innerHTML = '<div class="placeholder">Enter a scoring code to load players.</div>';
    return;
  }
  state.players.forEach((p) => {
    const row = makeScoreRow(p);
    if (p.id === state.selectedPlayerId) {
      row.classList.add("selected");
    }
    list.appendChild(row);
  });
}

function setScoreForSelected(value) {
  if (!state.selectedPlayerId) return;
  state.scores[state.selectedPlayerId] = value;
  renderScores();
}

function appendDigit(d) {
  if (!state.selectedPlayerId) return;
  const current = String(state.scores[state.selectedPlayerId] ?? "");
  if (current.toUpperCase() === "X") {
    state.scores[state.selectedPlayerId] = String(d);
  } else {
    const next = (current + String(d)).slice(0, 2);
    state.scores[state.selectedPlayerId] = next;
  }
  renderScores();
}

function clearSelected() {
  if (!state.selectedPlayerId) return;
  state.scores[state.selectedPlayerId] = "";
  renderScores();
}

function buildKeypad() {
  const keys = [
    "1","2","3",
    "4","5","6",
    "7","8","9",
    "Clear","0","X"
  ];
  const kp = document.getElementById("keypad");
  if (!kp) return;
  kp.innerHTML = "";

  keys.forEach((k) => {
    const div = document.createElement("div");
    div.className = "key";
    div.textContent = k;

    if (k === "Clear" || k === "X") {
      div.classList.add("small");
    }

    div.addEventListener("click", () => {
      if (k === "Clear") return clearSelected();
      if (k === "X") return setScoreForSelected("X");
      appendDigit(k);
    });

    kp.appendChild(div);
  });
}

function undoChanges() {
  state.scores = { ...state.originalScores };
  renderScores();
}

function parseHoleScore(value) {
  const normalized = String(value ?? "").trim();
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isInteger(parsed) ? parsed : null;
}

async function saveCurrentHole() {
  if (!state.matchId) {
    alert("Load a match code first.");
    return;
  }
  if (!state.hole) {
    alert("Select a hole to save.");
    return;
  }
  const playerA = state.players[0];
  const playerB = state.players[1];
  if (!playerA || !playerB) {
    alert("Match requires two players.");
    return;
  }
  const aScore = parseHoleScore(state.scores[playerA.id]);
  const bScore = parseHoleScore(state.scores[playerB.id]);
  if (aScore === null || bScore === null) {
    alert("Enter valid scores for both players before saving.");
    return;
  }
  const payload = {
    holes: [
      {
        hole_number: state.hole.number,
        player_a_score: aScore,
        player_b_score: bScore,
      },
    ],
  };
  const response = await fetch(`/matches/${state.matchId}/holes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const result = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = result?.detail || "Unable to save scores.";
    alert(detail);
    return;
  }
  state.originalScores = { ...state.scores };
  const holeEntry = state.holes[state.holeIndex];
  holeEntry.player_scores = [
    { ...(holeEntry.player_scores?.[0] || {}), gross: aScore },
    { ...(holeEntry.player_scores?.[1] || {}), gross: bScore },
  ];
  alert("Scores saved.");
}

async function fetchScorecard(params) {
  const url = new URL("/api/match_scorecard", window.location.origin);
  if (params.match_code) {
    url.searchParams.set("match_code", params.match_code);
  } else if (params.match_key) {
    url.searchParams.set("match_key", params.match_key);
  }
  const response = await fetch(url);
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = payload?.detail || `Match lookup failed (${response.status})`;
    const error = new Error(detail);
    error.status = response.status;
    throw error;
  }
  return payload;
}

async function loadMatchByIdentifier(identifier) {
  let lastError;
  try {
    return await fetchScorecard({ match_code: identifier });
  } catch (error) {
    lastError = error;
    if (identifier.includes("-") || error?.status === 404) {
      return await fetchScorecard({ match_key: identifier });
    }
    throw error;
  }
}

function initializeMatchFromData(data, fallbackCode) {
  if (!data?.match_id) {
    throw new Error("Match data is incomplete.");
  }
  const players = (data.players || []).map((player, idx) => ({
    id: `player_${idx}`,
    name: player.name || `Player ${idx + 1}`,
    handicap: player.course_handicap ?? 0,
  }));
  if (!players.length || !Array.isArray(data.holes) || !data.holes.length) {
    throw new Error("Match lacks player or hole data.");
  }
  state.matchId = data.match_id;
  state.matchKey = data.match_key;
  state.matchCode = data.match_code || fallbackCode || "";
  state.players = players;
  state.holes = data.holes;
  goToHoleIndex(0);
  hideCodeOverlay();
}

function goToHoleIndex(index) {
  if (!state.holes.length) return;
  const safeIndex = Math.max(0, Math.min(index, state.holes.length - 1));
  state.holeIndex = safeIndex;
  const entry = state.holes[safeIndex] || {};
  state.hole = {
    number: entry.hole_number,
    par: entry.par,
    handicap: entry.handicap,
  };
  state.selectedPlayerId = state.players[0]?.id || null;
  const scores = entry.player_scores || [];
  state.scores = {};
  state.players.forEach((player, idx) => {
    const raw = scores[idx]?.gross;
    state.scores[player.id] = raw !== undefined && raw !== null ? String(raw) : "";
  });
  state.originalScores = { ...state.scores };
  renderHole();
  renderScores();
}

async function handleCodeSubmit(prefilledValue) {
  const value = (prefilledValue ?? codeInput.value ?? "").trim();
  if (!value) {
    showCodeError("Enter a scoring code.");
    return;
  }
  showCodeError("");
  if (codeSubmitBtn) {
    codeSubmitBtn.disabled = true;
  }
  try {
    const data = await loadMatchByIdentifier(value);
    initializeMatchFromData(data, value);
  } catch (error) {
    const message = error?.message || "Unable to load the match.";
    showCodeError(message);
  } finally {
    if (codeSubmitBtn) {
      codeSubmitBtn.disabled = false;
    }
  }
}

function navigateHole(offset) {
  goToHoleIndex(state.holeIndex + offset);
}

function init() {
  buildKeypad();
  renderScores();
  document.getElementById("undoBtn")?.addEventListener("click", undoChanges);
  document.getElementById("saveBtn")?.addEventListener("click", saveCurrentHole);
  document.getElementById("prevHole")?.addEventListener("click", () => navigateHole(-1));
  document.getElementById("nextHole")?.addEventListener("click", () => navigateHole(1));
  codeForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    handleCodeSubmit();
  });
  codeSubmitBtn?.addEventListener("click", (event) => {
    event.preventDefault();
    handleCodeSubmit();
  });
  const params = new URLSearchParams(window.location.search);
  const candidate = params.get("match_code") || params.get("match_key");
  if (candidate) {
    codeInput.value = candidate;
    handleCodeSubmit(candidate);
  }
}

init();
