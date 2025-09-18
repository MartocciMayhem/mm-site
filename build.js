const fs = require("fs");
const path = require("path");

const dist = path.resolve(__dirname, "dist");
if (!fs.existsSync(dist)) fs.mkdirSync(dist, { recursive: true });

const html = `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>mm-site</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>body{font-family:system-ui;padding:2rem}</style>
  </head>
  <body>
    <h1>mm-site build ✓</h1>
    <p>${new Date().toISOString()}</p>
  </body>
</html>`;
fs.writeFileSync(path.join(dist, "index.html"), html, "utf8");
console.log("mm-site build completed. Output in ./dist");
