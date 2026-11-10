// Render react-icons → PNG base64 (một lần trước khi dựng slide).
const React = require("react");
const { renderToStaticMarkup } = require("react-dom/server");
const sharp = require("sharp");
const fa = require("react-icons/fa");

const NAMES = {
  warn: "FaExclamationTriangle", farm: "FaSeedling", truck: "FaTruck", db: "FaDatabase", clock: "FaClock",
  layers: "FaLayerGroup", check: "FaCheckCircle", rain: "FaCloudShowersHeavy", brain: "FaBrain", bell: "FaBell",
  mountain: "FaMountain", grid: "FaThLarge", search: "FaSearch",
};

async function renderIcons(color = "#FFFFFF") {
  const out = {};
  for (const [k, name] of Object.entries(NAMES)) {
    const svg = renderToStaticMarkup(React.createElement(fa[name], { size: 256, color }));
    const png = await sharp(Buffer.from(svg)).resize(256, 256).png().toBuffer();
    out[k] = "image/png;base64," + png.toString("base64");
  }
  return out;
}

module.exports = { renderIcons };
