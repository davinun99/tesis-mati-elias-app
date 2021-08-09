/*Onload de la Página*/
$(function () {
  $.get(api + "/reglas", function (fileNames) {
    for (let item of fileNames) {
      $("#filesList").append(
        $("<a>", {
          href: "#",
          class: "list-group-item list-group-item-action list-group-item-light",
          text: item,
        })
      );
    }
  }).fail(function () {
    /*Error de Conexion al servidor */
    console.error("error get");
  });

  $("#filesList").on("click", "a", function (e) {
    let fileName = e.target.innerText;
    fetch(api + "/reglas/" + fileName)
      .then((resp) => resp.blob())
      .then((blob) => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.style.display = "none";
        a.href = url;
        a.download = fileName;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
      })
      .catch(() => alert("No se pudo descargar archivo"));
  });
});
