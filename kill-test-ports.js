const fkill = require('fkill');

(async () => {
  const ports = [];
  for (let port = 8000; port < 9000; port++) {
    ports.push(`:${port}`);
  }
  await fkill(ports, { force: true, silent: true });
})();
