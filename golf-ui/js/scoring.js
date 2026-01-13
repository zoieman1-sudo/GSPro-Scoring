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
  matches: [],
  matchHoleEntries: {},
  courseName: "Scoring",
};

const overlay = document.getElementById("codeOverlay");
const codeForm = document.getElementById("codeForm");
const codeInput = document.getElementById("matchCodeInput");
const codeSubmitBtn = document.getElementById("matchCodeSubmit");
const codeError = document.getElementById("codeError");
const scoreboardList = document.getElementById("scoreList");
const courseTitle = document.getElementById("courseTitle");
const currentHoleLabel = document.getElementById("currentHoleLabel");
const scoreboardHoleMeta = document.getElementById("scoreboardHoleMeta");

let advanceTimerId = null;

function clearAdvanceTimer() {
  if (advanceTimerId) {
    window.clearTimeout(advanceTimerId);
    advanceTimerId = null;
  }
}

function focusSaveButton() {
  document.getElementById("saveBtn")?.focus();
}

function advanceToNextPlayer() {
  clearAdvanceTimer();
  if (!state.players.length || !state.selectedPlayerId) {
    return;
  }
  const currentIndex = state.players.findIndex((player) => player.id === state.selectedPlayerId);
  if (currentIndex === -1) {
    return;
  }
  const nextPlayer = state.players[currentIndex + 1];
  if (nextPlayer) {
    state.selectedPlayerId = nextPlayer.id;
    renderScores();
    return;
  }
  focusSaveButton();
}

function scheduleAdvanceAfterInput() {
  clearAdvanceTimer();
  advanceTimerId = window.setTimeout(() => {
    advanceToNextPlayer();
  }, 400);
}

function showCodeError(message) {
  if (codeError) {
    codeError.textContent = message || "";
  }
}

function hideCodeOverlay() {
  overlay?.classList.add("hidden");
}

function showCodeOverlay() {
  overlay?.classList.remove("hidden");
  codeInput?.focus();
}

function updateCourseTitle(name) {
  if (courseTitle) {
    courseTitle.textContent = name || "Scoring";
  }
}

function renderHole() {
  const hole = state.hole;
  const holeText = document.getElementById("holeText");
  const holeMeta = document.getElementById("holeMeta");
  if (!hole) {
    if (holeText) holeText.textContent = "Hole —";
    if (holeMeta) holeMeta.textContent = "Par —";
    if (currentHoleLabel) currentHoleLabel.textContent = "Hole —";
    if (scoreboardHoleMeta) scoreboardHoleMeta.textContent = "Par —";
    return;
  }
  const holeLabel = `Hole ${hole.number}`;
  if (holeText) holeText.textContent = holeLabel;
  if (currentHoleLabel) currentHoleLabel.textContent = holeLabel;
  const hcpLabel = hole.handicap !== undefined ? hole.handicap : "—";
  const metaText = `Par ${hole.par} • HCP ${hcpLabel}`;
  if (holeMeta) holeMeta.textContent = metaText;
  if (scoreboardHoleMeta) scoreboardHoleMeta.textContent = metaText;
  updateHoleNavLabels();
}

function updateHoleNavLabels() {
  const prev = document.getElementById("prevHole");
  const next = document.getElementById("nextHole");
  if (!state.holes.length) {
    if (prev) {
      prev.textContent = "< Hole —";
      prev.classList.add("disabled");
    }
    if (next) {
      next.textContent = "Hole — >";
      next.classList.add("disabled");
    }
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

function renderScores() {
  if (!scoreboardList) return;
  scoreboardList.innerHTML = "";
  if (!state.players.length) {
    const placeholder = document.createElement("div");
    placeholder.className = "placeholder";
    placeholder.textContent = "Enter a scoring code to load players.";
    scoreboardList.appendChild(placeholder);
    return;
  }
  state.players.forEach((player) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "player-card";
    button.dataset.playerId = player.id;
    const value = state.scores[player.id] ?? "";
    button.innerHTML = `
      <div class="player-card__info">
        <span class="player-card__name">${player.name}</span>
        <span class="player-card__hcp">(${player.handicap})</span>
      </div>
      <div class="player-card__score">${value || ""}</div>
    `;
    if (player.id === state.selectedPlayerId) {
      button.classList.add("selected");
    }
    button.addEventListener("click", () => {
      clearAdvanceTimer();
      state.selectedPlayerId = player.id;
      renderScores();
    });
    scoreboardList.appendChild(button);
  });
}

function setScoreForSelected(value) {
  if (!state.selectedPlayerId) return;
  state.scores[state.selectedPlayerId] = value;
  renderScores();
  advanceToNextPlayer();
}

function appendDigit(digit) {
  if (!state.selectedPlayerId) return;
  const current = String(state.scores[state.selectedPlayerId] ?? "");
  if (current.toUpperCase() === "X") {
    state.scores[state.selectedPlayerId] = String(digit);
  } else {
    const next = (current + String(digit)).slice(0, 2);
    state.scores[state.selectedPlayerId] = next;
  }
  renderScores();
  scheduleAdvanceAfterInput();
}

function clearSelected() {
  if (!state.selectedPlayerId) return;
  state.scores[state.selectedPlayerId] = "";
  renderScores();
  clearAdvanceTimer();
}

function buildKeypad() {
  const keys = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "Clear", "0", "X"];
  const kp = document.getElementById("keypad");
  if (!kp) return;
  kp.innerHTML = "";
  keys.forEach((key) => {
    const button = document.createElement("div");
    button.className = "key";
    button.textContent = key;
    if (key === "Clear" || key === "X") {
      button.classList.add("small");
    }
    button.addEventListener("click", () => {
      if (key === "Clear") return clearSelected();
      if (key === "X") return setScoreForSelected("X");
      appendDigit(key);
    });
    kp.appendChild(button);
  });
}

function loadScoresForHole(index) {
  if (!state.players.length) return;
  state.players.forEach((player) => {
    const matchHoles = state.matchHoleEntries[player.matchKey] || [];
    const holeEntry = matchHoles[index] || {};
    const playerScores = holeEntry.player_scores || [];
    const raw = playerScores[player.playerIndex]?.gross;
    state.scores[player.id] = raw !== undefined && raw !== null ? String(raw) : "";
  });
  state.originalScores = { ...state.scores };
  if (!state.selectedPlayerId) {
    state.selectedPlayerId = state.players[0]?.id || null;
  }
}

function parseHoleScore(value) {
  const normalized = String(value ?? "").trim();
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isInteger(parsed) ? parsed : null;
}

async function saveCurrentHole() {
  if (!state.matches.length) {
    alert("Load a match code first.");
    return;
  }
  if (!state.hole) {
    alert("Select a hole to save.");
    return;
  }
  const holeNumber = state.hole.number;
  for (const match of state.matches) {
    const matchPlayers = state.players.filter((player) => player.matchKey === match.match_key);
    if (!matchPlayers.length) {
      alert("Each match requires players before scoring.");
      return;
    }
    const teamTotals = new Map();
    const perPlayerScores = {};
    for (const player of matchPlayers) {
      const scoreValue = parseHoleScore(state.scores[player.id]);
      if (scoreValue === null) {
        alert("Enter valid scores for all players before saving.");
        return;
      }
      perPlayerScores[player.playerIndex] = scoreValue;
      const teamIndex =
        typeof player.teamIndex === "number" ? player.teamIndex : player.playerIndex < 2 ? 0 : 1;
      teamTotals.set(teamIndex, (teamTotals.get(teamIndex) ?? 0) + scoreValue);
    }
    const aScore = teamTotals.get(0);
    const bScore = teamTotals.get(1);
    if (aScore === undefined || bScore === undefined) {
      alert("Each match needs two teams to submit scores.");
      return;
    }
    const payload = {
      holes: [
        {
          hole_number: holeNumber,
          player_a_score: perPlayerScores[0] ?? 0,
          player_b_score: perPlayerScores[1] ?? 0,
          player_c_score: perPlayerScores[2] ?? 0,
          player_d_score: perPlayerScores[3] ?? 0,
        },
      ],
    };
    const response = await fetch(`/matches/${match.match_id}/holes`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const result = await response.json().catch(() => null);
    if (!response.ok) {
      const detail = result?.detail || `Unable to save scores for ${match.match_key}.`;
      alert(detail);
      return;
    }
    state.matchHoleEntries[match.match_key] = state.matchHoleEntries[match.match_key] || [];
    const chapter = state.matchHoleEntries[match.match_key][state.holeIndex] || {};
    const existingScores = chapter.player_scores || [];
    const updatedPlayerScores = [0, 1, 2, 3].map((index) => ({
      ...(existingScores[index] || {}),
      gross: perPlayerScores[index] ?? 0,
      net: perPlayerScores[index] ?? 0,
    }));
    chapter.player_scores = updatedPlayerScores;
    state.matchHoleEntries[match.match_key][state.holeIndex] = chapter;
  }
  state.originalScores = { ...state.scores };
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

function initializeMatchFromScorecard(scorecard, fallbackValue) {
  if (!scorecard || !scorecard.match_key) {
    throw new Error("Scorecard data is incomplete.");
  }
  const matchKey = scorecard.match_key;
  const players = (scorecard.players || []).map((player, idx) => {
    const teamIndex =
      typeof player.team_index === "number"
        ? player.team_index
        : idx < 2
        ? 0
        : 1;
    const role = player.role || (teamIndex === 0 ? "A" : "B");
    return {
      id: `${matchKey}-player-${idx}`,
      matchKey,
      matchId: scorecard.match_id,
      name: player.name || `Player ${idx + 1}`,
      handicap: player.course_handicap ?? 0,
      role,
      teamIndex,
      playerIndex: idx,
    };
  });
  state.matches = [
    {
      match_key: matchKey,
      match_id: scorecard.match_id,
      match_code: scorecard.match_code,
    },
  ];
  state.matchHoleEntries = {
    ...state.matchHoleEntries,
    [matchKey]: Array.isArray(scorecard.holes) ? scorecard.holes : [],
  };
  state.players = players;
  state.holes = state.matchHoleEntries[matchKey];
  state.matchCode = fallbackValue || scorecard.match_code || "";
  state.selectedPlayerId = players[0]?.id || null;
  state.holeIndex = 0;
  state.scores = {};
  state.originalScores = {};
  state.courseName = scorecard.course?.course_name || "Scoring";
  hideCodeOverlay();
  updateCourseTitle(state.courseName);
  goToHoleIndex(0);
  renderScores();
}

function updateScoresFromServer() {
  if (!state.players.length) return;
  const holeIndex = state.holeIndex;
  let refreshed = false;
  state.players.forEach((player) => {
    const holeEntry = state.matchHoleEntries[player.matchKey]?.[holeIndex] || {};
    const playerScores = holeEntry.player_scores || [];
    const gross = playerScores[player.playerIndex]?.gross;
    const serverValue = gross !== undefined && gross !== null ? String(gross) : "";
    const currentValue = state.scores[player.id] ?? "";
    const originalValue = state.originalScores[player.id] ?? "";
    if (currentValue === originalValue && serverValue !== currentValue) {
      state.scores[player.id] = serverValue;
      state.originalScores[player.id] = serverValue;
      refreshed = true;
    }
  });
  if (refreshed) {
    renderScores();
  }
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
  loadScoresForHole(safeIndex);
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
    initializeMatchFromScorecard(data, value);
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

async function loadActiveMatch() {
  try {
    const response = await fetch("/api/active_match");
    if (!response.ok) {
      return;
    }
    const payload = await response.json().catch(() => null);
    if (payload?.active_match) {
      initializeMatchFromScorecard(payload.active_match);
    }
  } catch (error) {
    console.warn("Unable to load active match", error);
  }
}

function init() {
  buildKeypad();
  renderScores();
  document.getElementById("undoBtn")?.addEventListener("click", () => {
    state.scores = { ...state.originalScores };
    renderScores();
  });
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
  } else {
    loadActiveMatch();
  }
}

init();
