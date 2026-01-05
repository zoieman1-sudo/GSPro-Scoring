async function fetchRound() {
  return { id: "round_123", title: "2025 Stag Day", date: "2025-08-28" };
}

async function fetchPlayers(roundId) {
  return [
    { id: "p1", firstName: "Paul", lastName: "OLIVER", handicap: 14 },
    { id: "p2", firstName: "Gary", lastName: "LONG", handicap: 10 },
    { id: "p3", firstName: "Alcarese,", lastName: "ANDREW", handicap: 9 },
    { id: "p4", firstName: "Leonhardt,", lastName: "BRETT", handicap: 8 },
  ];
}

function formatDateParts(iso) {
  const d = new Date(`${iso}T00:00:00`);
  const dow = d.toLocaleDateString(undefined, { weekday: "short" }).toUpperCase();
  const day = d.getDate();
  const mon = d.toLocaleDateString(undefined, { month: "short" }).toUpperCase();
  return { dow, day, mon };
}

function makeTile(player) {
  const btn = document.createElement("button");
  btn.className = "tile";
  btn.type = "button";
  btn.setAttribute("aria-label", `${player.firstName} ${player.lastName}`);

  btn.innerHTML = `
    <div>
      <div class="first">${player.firstName}</div>
      <div class="last">${player.lastName}</div>
    </div>
  `;

  btn.addEventListener("click", () => {
    const params = new URLSearchParams({
      round_id: window.__round.id,
      player_id: player.id,
    });
    window.location.href = `scoring.html?${params.toString()}`;
  });

  return btn;
}

async function init() {
  const round = await fetchRound();
  window.__round = round;

  document.getElementById("roundTitle").textContent = round.title;

  const parts = formatDateParts(round.date);
  document.getElementById("dowText").textContent = `${parts.dow},`;
  document.getElementById("dayText").textContent = `${parts.day}`;
  document.getElementById("monText").textContent = `${parts.mon}.`;

  const players = await fetchPlayers(round.id);
  const tiles = document.getElementById("tiles");
  tiles.innerHTML = "";
  players.forEach((p) => tiles.appendChild(makeTile(p)));
}

init().catch(console.error);
