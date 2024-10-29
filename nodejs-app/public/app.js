// Initialize the map with Leaflet
const map = L.map('map').setView([51.505, -0.09], 13); // Set starting location
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
}).addTo(map);

// Layer groups for each dataset
const dataLayers = {
    data1: L.layerGroup().addTo(map),
    data2: L.layerGroup().addTo(map),
    data3: L.layerGroup().addTo(map),
};

// Handle checkbox changes
document.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
    checkbox.addEventListener('change', async () => {
        const dataset = checkbox.value;
        if (checkbox.checked) {
            // Fetch data when checked
            const data = await fetchData(dataset);
            plotData(data, dataset);
        } else {
            // Clear markers if unchecked
            dataLayers[dataset].clearLayers();
        }
    });
});

// Fetch data from backend
async function fetchData(dataset) {
    try {
        const response = await fetch(`/api/data?dataset=${dataset}`);
        return response.ok ? await response.json() : [];
    } catch (error) {
        console.error("Fetch error:", error);
        return [];
    }
}

// Plot data on the map
function plotData(data, dataset) {
    data.forEach(point => {
        const marker = L.marker([point.latitude, point.longitude]);
        marker.addTo(dataLayers[dataset]);
    });
}
