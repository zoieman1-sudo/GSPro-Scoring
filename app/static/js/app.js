const form = document.querySelector(".score-form");
const matchDataEl = document.getElementById("match-data");
const matchSelect = document.querySelector(".match-dropdown");
const playerNameA = document.querySelector('[data-player-name="A"]');
const playerNameB = document.querySelector('[data-player-name="B"]');
const hiddenMatchName = form ? form.querySelector('input[name="match_name"]') : null;
const hiddenPlayerA = form ? form.querySelector('input[name="player_a"]') : null;
const hiddenPlayerB = form ? form.querySelector('input[name="player_b"]') : null;
let matches = [];

if (matchDataEl) {
  try {
    matches = JSON.parse(matchDataEl.textContent);
  } catch (error) {
    matches = [];
  }
}

const applyMatch = (matchId) => {
  const match = matches.find((item) => item.match_id === matchId);
  if (!match) {
    return;
  }
  if (playerNameA) {
    playerNameA.textContent = match.player_a;
  }
  if (playerNameB) {
    playerNameB.textContent = match.player_b;
  }
  if (hiddenMatchName) {
    hiddenMatchName.value = match.match_name;
  }
  if (hiddenPlayerA) {
    hiddenPlayerA.value = match.player_a;
  }
  if (hiddenPlayerB) {
    hiddenPlayerB.value = match.player_b;
  }
};

if (matchSelect) {
  applyMatch(matchSelect.value);
  matchSelect.addEventListener("change", (event) => {
    applyMatch(event.target.value);
  });
}

const showStatus = (message) => {
  let status = document.querySelector(".status");
  if (!status) {
    status = document.createElement("div");
    status.className = "status";
    form.before(status);
  }
  status.textContent = message;
};

if (form) {
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const payload = {
      match_id: formData.get("match_id"),
      match_name: formData.get("match_name"),
      player_a: formData.get("player_a"),
      player_b: formData.get("player_b"),
      player_a_points: Number(formData.get("player_a_points")),
      player_b_points: Number(formData.get("player_b_points")),
    };

    try {
      const response = await fetch("/api/scores", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await response.json();
      if (!response.ok) {
        showStatus(data.error || "Unable to submit match.");
        return;
      }
      showStatus(`Saved. Winner: ${data.winner}. Totals ${data.player_a_total}-${data.player_b_total}.`);
      form.reset();
    } catch (error) {
      showStatus("Network error. Submit again or use the form fallback.");
      form.submit();
    }
  });
}
