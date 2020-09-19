function DefinirElementosRedFlags() {
  $(".redFlags.informacion").append(
    $("<div>", { class: "row" }).append(
      $("<h4>", {
        class: "col-6 col-sm-6 col-md-6 titularCajonSombreado",
        text: "Datos de Banderas Rojas",
      })
    ),
    $("<div>", {
      class: "cajonSombreadox contenedorDetalleProcesoDatos",
    }).append(
      $("<nav>").append(
        $("<div>", { class: "nav nav-tabs", role: "tablist" }).append(
          $("<a>", {
            class: "nav-item nav-link active",
            "data-toggle": "tab",
            role: "tab",
            "aria-controls": "informacionTabRedFlags",
            href: "#informacionTabRedFlags",
            "aria-selected": "true",
          }).append(
            $("<h4>", {
              class: "titularColor",
              style: "font-size: 15px",
              text: "Información",
            })
          )
        )
      ),
      $("<div>", {
        class: "tab-content cajonSombreado",
        id: "contenedorTabRedFlags",
      }).append(
        $("<div>", {
          class: "tab-pane fade show active",
          role: "tabpanel",
          "aria-labelledby": "informacionTabRedFlags",
          id: "informacionTabRedFlags",
        }).append(
          $("<div>", {
            class: "contenedorProceso informacionProceso",
          }).append(
            $("<div>", { class: "contenedorTablaCaracteristicas" }).append(
              $("<table>").append(
                procesoRedFlags && procesoRedFlags.length > 0
                  ? $("<tbody>").append(
                      $(
                        '<tr><td><h4 class="titularColor textoColorPrimario">Banderas Encontradas</h4></td></tr>'
                      ),
                      ObtenerRedFlags()
                    )
                  : $("<tbody>").append(
                      $(
                        '<tr><td><h4 class="titularColor textoColorPrimario">No se encontraron banderas</h4></td></tr>'
                      )
                    )
              )
            )
          )
        )
      )
    )
  );
}

function ObtenerRedFlags() {
  var elementos = [];
  for (var i = 0; i < procesoRedFlags.length; i++) {
    elementos.push(
      $("<tr>").append(
        $("<td>", {
          class: "tituloTablaCaracteristicas",
          text: procesoRedFlags[i].title,
        }),
        $("<td>", {
          class: "contenidoTablaCaracteristicas",
          text: procesoRedFlags[i].message,
        })
      )
    );
  }
  return elementos;
}
