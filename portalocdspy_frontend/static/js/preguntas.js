/**
 * @file preguntas.js Este archivo se incluye en la sección de Preguntas del Portal de Contrataciones Abiertas de Honduras
 * @author Bryant Marcelo Pérez
 * @see <a href="https://github.com/portaledcahn/edcahn_backend/tree/frontend">GitHub</a>
 */

/**Objeto de ejemplo
 * @type {Object}
 */
json={
    "ocid": "ocds-213czf-000-00001",
    "id": "ocds-213czf-000-00001-05-contract",
    "date": "2010-05-10T10:30:00Z",
    "language": "en",
    "tag": [
    "contract"
    ],
    "initiationType": "tender",
    "parties": [
    {
        "identifier": {},
        "name": "London Borough of Barnet",
        "address": {},
        "contactPoint": {},
        "roles": [],
        "id": "GB-LAC-E09000003"
    },
    {
        "identifier": {},
        "additionalIdentifiers": [],
        "name": "AnyCorp Cycle Provision",
        "address": {},
        "contactPoint": {},
        "roles": [],
        "id": "GB-COH-1234567844"
    }]
};

/**
 * Inicializa la vista del json
 */
$(function(){
  $('#jsonEjemplo').jsonview(json);
});
