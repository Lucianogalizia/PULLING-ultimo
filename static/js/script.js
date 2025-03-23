// Archivo: static/js/script.js

document.addEventListener('DOMContentLoaded', function() {
  console.log("¡Custom JavaScript cargado!");

  // Puedes agregar aquí funcionalidades personalizadas.
  // Ejemplo: mostrar un mensaje emergente al hacer clic en algún botón.
  const buttons = document.querySelectorAll('.btn');
  buttons.forEach(btn => {
    btn.addEventListener('click', function() {
      console.log(`Se hizo clic en: ${this.textContent.trim()}`);
    });
  });
});
