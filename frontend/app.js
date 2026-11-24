const statusElement = document.querySelector("#status");
const versionElement = document.querySelector("#version");
const rangesBody = document.querySelector("#ranges-body");
const lookupResult = document.querySelector("#lookup-result");

function formatBoundary(encodedKey, isStart) {
  if (!encodedKey) return isStart ? "-infinity" : "+infinity";
  return decodeBase64(encodedKey);
}

function decodeBase64(value) {
  const padded = value.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(value.length / 4) * 4, "=");
  return new TextDecoder().decode(Uint8Array.from(atob(padded), (character) => character.charCodeAt(0)));
}

function encodeKey(value) {
  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function renderRanges(ranges) {
  if (ranges.length === 0) {
    rangesBody.innerHTML = '<tr><td colspan="5" class="empty">No ranges have been registered.</td></tr>';
    return;
  }

  rangesBody.replaceChildren(...ranges.map((range) => {
    const row = document.createElement("tr");
    const values = [
      range.id,
      formatBoundary(range.startKey, true),
      formatBoundary(range.endKey, false),
      (range.replicaIds || []).join(", ") || "-",
      range.leaderId || "Unknown",
    ];
    for (const value of values) {
      const cell = document.createElement("td");
      cell.textContent = String(value);
      row.append(cell);
    }
    return row;
  }));
}

async function refreshMetadata() {
  statusElement.textContent = "Loading metadata...";
  try {
    const response = await fetch("/api/metadata/ranges", { headers: { Accept: "application/json" } });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
    versionElement.textContent = `Version ${data.version}`;
    renderRanges(data.ranges || []);
    statusElement.textContent = "Metadata loaded.";
    statusElement.dataset.state = "success";
  } catch (error) {
    statusElement.textContent = `Could not load metadata: ${error.message}`;
    statusElement.dataset.state = "error";
  }
}

document.querySelector("#refresh-button").addEventListener("click", refreshMetadata);

document.querySelector("#lookup-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const key = document.querySelector("#key-input").value;
  lookupResult.hidden = false;
  lookupResult.textContent = "Looking up key...";
  try {
    const response = await fetch(`/api/metadata/lookup?key=${encodeKey(key)}`, {
      headers: { Accept: "application/json" },
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
    lookupResult.textContent = JSON.stringify(data, null, 2);
  } catch (error) {
    lookupResult.textContent = error.message;
  }
});

refreshMetadata();
