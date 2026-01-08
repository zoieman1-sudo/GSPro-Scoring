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
  groupKey: null,
  groupLabel: "",
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
const groupLabelElement = document.getElementById("groupLabel");
const courseTitle = document.getElementById("courseTitle");
const currentHoleLabel = document.getElementById("currentHoleLabel");
const scoreboardHoleMeta = document.getElementById("scoreboardHoleMeta");

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

function updateGroupLabel(label) {
  if (groupLabelElement) {
    groupLabelElement.textContent = label || "";
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
      <div class="player-card__score-group">
        <span class="player-card__score">${value || ""}</span>
        <span class="player-card__dot"></span>
      </div>
    `;
    if (player.id === state.selectedPlayerId) {
      button.classList.add("selected");
    }
    button.addEventListener("click", () => {
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
}

function clearSelected() {
  if (!state.selectedPlayerId) return;
  state.scores[state.selectedPlayerId] = "";
  renderScores();
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
    if (matchPlayers.length < 2) {
      alert("Each match requires two players.");
      return;
    }
    const playerA = matchPlayers[0];
    const playerB = matchPlayers[1];
    const aScore = parseHoleScore(state.scores[playerA.id]);
    const bScore = parseHoleScore(state.scores[playerB.id]);
    if (aScore === null || bScore === null) {
      alert("Enter valid scores for all players before saving.");
      return;
    }
    const payload = {
      holes: [
        {
          hole_number: holeNumber,
          player_a_score: aScore,
          player_b_score: bScore,
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
    chapter.player_scores = [
      { ...(existingScores[0] || {}), gross: aScore },
      { ...(existingScores[1] || {}), gross: bScore },
    ];
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

async function fetchGroupScorecard(groupKey) {
  const url = `/api/match_groups/${encodeURIComponent(groupKey)}/scorecard`;
  const response = await fetch(url);
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = payload?.detail || `Group lookup failed (${response.status})`;
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

function groupPayloadFromScorecard(scorecard) {
  return {
    group_key: `group-${scorecard.match_key}`,
    label: scorecard.label,
    course: scorecard.course,
    holes: scorecard.holes,
    matches: [
      {
        match_key: scorecard.match_key,
        match_id: scorecard.match_id,
        match_code: scorecard.match_code,
        label: scorecard.label,
        players: (scorecard.players || []).map((player, idx) => ({
          name: player.name,
          handicap: player.course_handicap ?? 0,
          role: idx === 0 ? "A" : "B",
        })),
        holes: scorecard.holes,
      },
    ],
  };
}

function initializeGroupFromScorecard(scorecard, fallbackValue) {
  const payload = groupPayloadFromScorecard(scorecard);
  initializeGroupFromData(payload, fallbackValue);
}

function initializeGroupFromData(groupData, fallbackValue) {
  if (!groupData?.matches?.length) {
    throw new Error("Group data is incomplete.");
  }
  const matches = groupData.matches.filter((match) => match.match_key && match.match_id);
  if (!matches.length) {
    throw new Error("Group has no valid matches.");
  }
  const aggregatedPlayers = [];
  const matchHoleEntries = {};
  matches.forEach((match) => {
    matchHoleEntries[match.match_key] = Array.isArray(match.holes) ? match.holes : [];
    (match.players || []).forEach((player, playerIndex) => {
      aggregatedPlayers.push({
        id: `${match.match_key}-player-${playerIndex}`,
        matchKey: match.match_key,
        matchId: match.match_id,
        name: player.name || `Player ${playerIndex + 1}`,
        handicap: player.handicap ?? 0,
        role: player.role || (playerIndex === 0 ? "A" : "B"),
        playerIndex,
      });
    });
  });
  state.groupKey = groupData.group_key || null;
  state.groupLabel = groupData.label || "";
  state.courseName = groupData.course?.course_name || state.courseName;
  state.matches = matches;
  state.matchHoleEntries = matchHoleEntries;
  state.players = aggregatedPlayers;
  state.holes = Array.isArray(groupData.holes) && groupData.holes.length
    ? groupData.holes
    : matchHoleEntries[matches[0].match_key] || [];
  state.matchCode = fallbackValue || matches[0].match_code || "";
  state.selectedPlayerId = aggregatedPlayers[0]?.id || null;
  state.holeIndex = 0;
  state.scores = {};
  state.originalScores = {};
  goToHoleIndex(0);
  hideCodeOverlay();
  updateCourseTitle(state.courseName);
  updateGroupLabel(state.groupLabel || state.groupKey || "");
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
    try {
      const groupData = await fetchGroupScorecard(value);
      initializeGroupFromData(groupData, value);
      return;
    } catch (groupError) {
      if (groupError.status !== 404) {
        throw groupError;
      }
    }
    const data = await loadMatchByIdentifier(value);
    initializeGroupFromScorecard(data, value);
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
      initializeGroupFromScorecard(payload.active_match);
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
  const candidate = params.get("match_code") || params.get("match_key") || params.get("group_key");
  if (candidate) {
    codeInput.value = candidate;
    handleCodeSubmit(candidate);
  } else {
    loadActiveMatch();
  }
}

init();
