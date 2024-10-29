// server.js
const express = require('express');
const axios = require('axios');
const path = require('path');

const app = express();
const PORT = 3000;

// Serve static files from "public" folder
app.use(express.static(path.join(__dirname, 'public')));

// API route to fetch data based on dataset query
app.get('/api/data', async (req, res) => {
    const dataset = req.query.dataset; // Retrieve dataset from query
    try {
        // Example: Replace with actual API endpoint
        const response = await axios.get(`https://api.example.com/${dataset}`);
        res.json(response.data);
    } catch (error) {
        console.error("API call failed:", error);
        res.status(500).send("Server error");
    }
});

app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
