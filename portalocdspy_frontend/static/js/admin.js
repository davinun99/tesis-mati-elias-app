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
    $.get(api + "/reglas/" + fileName, function (file) {
      console.log(file);
    });
  });
});
