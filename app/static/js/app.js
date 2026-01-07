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

const coursePopup = document.getElementById("course-popup");
const openCoursePopup = document.getElementById("open-course-popup");
const closeCoursePopup = document.getElementById("course-popup-close");
const courseSearchForm = document.getElementById("course-search-form");
const courseSearchInput = document.getElementById("course-search-input");
const courseResultsList = document.getElementById("course-results-list");
const coursePopupStatus = document.getElementById("course-popup-status");

const toggleCoursePopup = (open) => {
  if (!coursePopup) {
    return;
  }
  coursePopup.dataset.open = open ? "true" : "false";
  coursePopup.setAttribute("aria-hidden", !open);
  document.body.classList.toggle("popup-open", open);
  if (!open && courseResultsList) {
    courseResultsList.innerHTML = "";
  }
  if (!open && coursePopupStatus) {
    coursePopupStatus.textContent = "Enter a query to search the API.";
  }
};

const updateCourseStatus = (message, error = false) => {
  if (!coursePopupStatus) {
    return;
  }
  coursePopupStatus.textContent = message;
  coursePopupStatus.style.color = error ? "#ff8a68" : "var(--muted)";
};

const getCourseId = (course) => course?.id || course?.course_id || course?.courseId || course?.courseID || course?.uid;

const renderCourseResults = (courses, query) => {
  if (!courseResultsList) {
    return;
  }
  courseResultsList.innerHTML = "";
  if (!courses.length) {
    updateCourseStatus(query ? `No results for "${query}".` : "No courses found.");
    return;
  }
  updateCourseStatus(`Showing ${courses.length} result${courses.length === 1 ? "" : "s"}.`);
  courses.forEach((course) => {
    const courseId = getCourseId(course);
    if (!courseId) {
      return;
    }
    const item = document.createElement("li");
    item.className = "course-popup__item";

    const details = document.createElement("div");
    details.className = "course-popup__details";
    const title = document.createElement("strong");
    title.textContent = course?.course_name || course?.name || course?.course || "Untitled";
    const meta = document.createElement("span");
    const location = course?.location || {};
    const locationParts = [location.city || course?.city, location.state || course?.state, location.country || course?.country].filter(Boolean);
    meta.textContent = locationParts.length ? locationParts.join(", ") : "Location unavailable";
    details.append(title, meta);

    const addButton = document.createElement("button");
    addButton.className = "course-popup__add";
    addButton.type = "button";
    addButton.textContent = "Add";
    addButton.dataset.courseId = courseId;
    addButton.addEventListener("click", async () => {
      if (!addButton || !courseId) {
        return;
      }
      addButton.disabled = true;
      const originalText = addButton.textContent;
      addButton.textContent = "Adding…";
      try {
        const response = await fetch(`/api/courses/import/${courseId}`, {
          method: "POST",
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload?.detail || "Unable to import course.");
        }
        addButton.textContent = "Added";
        updateCourseStatus(`Course "${title.textContent}" added.`, false);
      } catch (error) {
        addButton.disabled = false;
        addButton.textContent = originalText;
        updateCourseStatus(error.message || "Import failed.", true);
      }
    });

    item.append(details, addButton);
    courseResultsList.append(item);
  });
};

if (openCoursePopup) {
  openCoursePopup.addEventListener("click", () => toggleCoursePopup(true));
}
if (closeCoursePopup) {
  closeCoursePopup.addEventListener("click", () => toggleCoursePopup(false));
}
if (coursePopup) {
  coursePopup.addEventListener("click", (event) => {
    if (event.target === coursePopup) {
      toggleCoursePopup(false);
    }
  });
}

if (courseSearchForm && courseSearchInput) {
  courseSearchForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const query = courseSearchInput.value.trim();
    if (!query) {
      updateCourseStatus("Type a term to start searching.", true);
      return;
    }
    updateCourseStatus("Searching…");
    try {
      const response = await fetch(`/api/courses/search?query=${encodeURIComponent(query)}`);
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload?.detail || "Search failed");
      }
      const courses = Array.isArray(payload?.courses)
        ? payload.courses
        : Array.isArray(payload?.data?.courses)
        ? payload.data.courses
        : [];
      renderCourseResults(courses, query);
    } catch (error) {
      updateCourseStatus(error.message || "Search failed.", true);
    }
  });
}
