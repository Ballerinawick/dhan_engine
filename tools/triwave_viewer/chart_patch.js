function chartBox(canvas) {
  const rect = canvas.getBoundingClientRect();
  const left = 52;
  const right = 58;
  const top = 14;
  const bottom = 34;
  return {
    x: left,
    y: top,
    w: Math.max(90, rect.width - left - right),
    h: Math.max(50, rect.height - top - bottom),
  };
}

function drawGrid(ctx, box) {
  ctx.strokeStyle = "rgba(255,255,255,.06)";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = box.y + (box.h / 4) * i;
    ctx.beginPath();
    ctx.moveTo(box.x, y);
    ctx.lineTo(box.x + box.w, y);
    ctx.stroke();
  }
  for (let i = 0; i <= 4; i++) {
    const x = box.x + (box.w / 4) * i;
    ctx.beginPath();
    ctx.moveTo(x, box.y);
    ctx.lineTo(x, box.y + box.h);
    ctx.stroke();
  }
  ctx.strokeStyle = "rgba(154,165,181,.34)";
  ctx.beginPath();
  ctx.moveTo(box.x, box.y);
  ctx.lineTo(box.x, box.y + box.h);
  ctx.lineTo(box.x + box.w, box.y + box.h);
  ctx.lineTo(box.x + box.w, box.y);
  ctx.stroke();
}

function drawAxis(ctx, box, min, max) {
  ctx.fillStyle = "#9aa5b5";
  ctx.font = "12px Segoe UI";
  ctx.textAlign = "left";
  ctx.textBaseline = "middle";
  for (let i = 0; i <= 4; i++) {
    const value = max - ((max - min) / 4) * i;
    const y = box.y + (box.h / 4) * i;
    ctx.fillText(value.toFixed(2), box.x + box.w + 8, y);
  }
}

if (typeof render === "function") {
  render();
}
