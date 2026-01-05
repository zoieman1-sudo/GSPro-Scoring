async function fetchHole(roundId) {
  return { number: 12, par: 5, yards: 533, tees: "BLU" };
}

async function fetchPlayersForRound(roundId) {
  return [
    { id: "p1", name: "Paul Oliver", handicap: 14 },
    { id: "p2", name: "Gary Long", handicap: 10 },
    { id: "p3", name: "Alcarese, Andrew", handicap: 9 },
    { id: "p4", name: "Leonhardt, Brett", handicap: 8 },
  ];
}

async function fetchExistingScores(roundId, holeNumber) {
  return { p1: "X", p2: "5", p3: "5", p4: "5" };
}

async function saveScores(roundId, holeNumber, scoresByPlayerId) {
  console.log("Saving", { roundId, holeNumber, scoresByPlayerId });
  return { ok: true };
}

const state = {
  roundId: null,
  hole: null,
  players: [],
  selectedPlayerId: null,
  scores: {},
  originalScores: {},
};

function qs() {
  const p = new URLSearchParams(window.location.search);
  return {
    roundId: p.get("round_id") || "round_123",
    playerId: p.get("player_id") || null,
  };
}

function renderHole() {
  document.getElementById("holeText").textContent = `Hole ${state.hole.number}`;
  document.getElementById("holeMeta").textContent = `Par ${state.hole.par} • ${state.hole.yards} yds (${state.hole.tees})`;
  document.getElementById("prevHole").textContent = `< Hole ${state.hole.number - 1}`;
  document.getElementById("nextHole").textContent = `Hole ${state.hole.number + 1} >`;
}

function makeScoreRow(player) {
  const row = document.createElement("div");
  row.className = "score-row";
  row.dataset.playerId = player.id;

  const val = state.scores[player.id] ?? "";

  row.innerHTML = `
    <div>
      <div class="name">${player.name} <span class="hcp">(${player.handicap})</span></div>
    </div>
    <div style="display:flex; align-items:center;">
      <div class="value" aria-label="Score">${val || ""}</div>
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
  list.innerHTML = "";
  state.players.forEach((p) => {
    const row = makeScoreRow(p);
    if (p.id === state.selectedPlayerId) row.classList.add("selected");
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
  const cur = String(state.scores[state.selectedPlayerId] ?? "");
  if (cur.toUpperCase() === "X") {
    state.scores[state.selectedPlayerId] = String(d);
  } else {
    const next = (cur + String(d)).slice(0, 2);
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
  kp.innerHTML = "";

  keys.forEach((k) => {
    const div = document.createElement("div");
    div.className = "key";
    div.textContent = k;

    if (k === "Clear") div.classList.add("small");
    if (k === "X") div.classList.add("small");

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

async function init() {
  const { roundId, playerId } = qs();
  state.roundId = roundId;

  state.hole = await fetchHole(roundId);
  state.players = await fetchPlayersForRound(roundId);

  const existing = await fetchExistingScores(roundId, state.hole.number);
  state.scores = { ...existing };
  state.originalScores = { ...existing };

  state.selectedPlayerId = playerId || state.players[0]?.id || null;

  renderHole();
  buildKeypad();
  renderScores();

  document.getElementById("undoBtn").addEventListener("click", undoChanges);

  document.getElementById("saveBtn").addEventListener("click", async () => {
    const res = await saveScores(state.roundId, state.hole.number, state.scores);
    if (!res?.ok) {
      alert("Save failed. Please try again.");
      return;
    }
    state.originalScores = { ...state.scores };
  });
}

init().catch(console.error);
