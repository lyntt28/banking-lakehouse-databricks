// Kiểm tra mọi spec Custom Viz (dashboards/custom_viz/**.json) compile được bằng Vega-Lite.
// CI: npm install vega-lite@5.21.0 && node tools/compile_vega_specs.js dashboards/custom_viz
const vl = require('vega-lite');
const fs = require('fs');
const path = require('path');

const root = process.argv[2] || 'dashboards/custom_viz';
let total = 0;
let bad = 0;
for (const dir of fs.readdirSync(root)) {
  const p = path.join(root, dir);
  if (!fs.statSync(p).isDirectory()) continue;
  for (const f of fs.readdirSync(p).filter((x) => x.endsWith('.json'))) {
    total += 1;
    try {
      const spec = JSON.parse(fs.readFileSync(path.join(p, f), 'utf8'));
      spec.data = { values: [] };   // dashboard truyền dữ liệu; chỉ kiểm tra cú pháp / ngữ nghĩa
      vl.compile(spec);
    } catch (e) {
      bad += 1;
      console.error(`FAIL ${dir}/${f}: ${e.message}`);
    }
  }
}
console.log(`Vega-Lite specs compiled: ${total - bad}/${total}`);
process.exit(bad || total === 0 ? 1 : 0);
