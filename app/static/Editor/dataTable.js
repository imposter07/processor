function selectColumns(editor, csv, header) {
    let selectEditor = new $.fn.dataTable.Editor();
    let fields = editor.order();

    for (let i = 0; i < fields.length; i++) {
        let field = editor.field(fields[i]);

        selectEditor.add({
            label: field.label(),
            name: field.name(),
            type: 'select',
            options: header,
            def: header[i]
        });
    }
    selectEditor.create({
        title: 'Map CSV fields',
        buttons: 'Import ' + csv.length + ' records',
        message: 'Select the CSV column you want to use the data from for each field.'
    });
    selectEditor.on('submitComplete', function (e, json, data, action) {
        // Use the host Editor instance to show a multi-row create form allowing the user to submit the data.
        editor.create(csv.length, {
            title: 'Confirm import',
            buttons: 'Submit',
            message: 'Click the <i>Submit</i> button to confirm the import of ' + csv.length + ' rows of data. Optionally, override the value for a field to set a common value by clicking on the field below.'
        });
        editor.on('close', function () {
            $('#tableModal').modal('show');
        });

        for (let i = 0; i < fields.length; i++) {
            let field = editor.field(fields[i]);
            let mapped = data[field.name()];

            for (let j = 0; j < csv.length; j++) {
                field.multiSet(j, csv[j][mapped]);
            }
        }

    });
}

function createTable(colData, rawData, tableName,
                     elem = "modal-body-table") {
    let cols = JSON.parse(colData);
    let tableFields = cols.map(function (e) {
        if (e === 'index') {
            return {label: e, name: e, type: "hidden"}
        } else {
            return {label: e, name: e}
        }
    });
    let tableCols = cols.map(function (e) {
        return {data: e}
    });
    let tableJquery = '#' + tableName;
    document.getElementById(elem).innerHTML = rawData;
    $(document).ready(function () {
        let editor = new $.fn.dataTable.Editor({
            table: tableJquery,
            idSrc: 'index',
            fields: tableFields
        });
        editor.on('open', function (e, type, mode, action) {
            if ((type === 'main') && (elem === 'modal-body-table')) {
                $('#tableModal').modal('hide');
            }
        });
        editor.on('close', function () {
            if (elem === 'modal-body-table') {
                $('#tableModal').modal('show');
            }
        });
        let uploadEditor = new $.fn.dataTable.Editor({
            fields: [{
                label: 'CSV file:',
                name: 'csv',
                type: 'upload',
                ajax: function (files) {
                    // Ajax override of the upload so we can handle the file locally. Here we use Papa
                    // to parse the CSV.
                    Papa.parse(files[0], {
                        header: true,
                        skipEmptyLines: true,
                        complete: function (results) {
                            if (results.errors.length) {
                                uploadEditor.field('csv').error('CSV parsing error: ' + results.errors[0].message);
                            } else {
                                uploadEditor.close();
                                selectColumns(editor, results.data, results.meta.fields);
                            }
                        }
                    });
                }
            }]
        });
        let dom = "<div class='row'><div class='col'>B</div><div class='col'>f</div></div>";
        var table = $(tableJquery).DataTable({
            dom: "Bfrtip",
            columns: tableCols,
            "deferRender": true,
            "orderClasses": false,
            "scrollX": true,
            responsive: true,
            keys: {
                columns: ':not(:first-child)',
                editor: editor
            },
            select: {
                style: 'os',
                selector: 'td:first-child',
                blurable: true
            },
            buttons: [
                {
                    extend: 'searchPanes',
                    config: {
                        cascadePanes: true,
                        viewTotal: true,
                        layout: 'columns-3'
                    }
                },
                {
                    extend: 'collection',
                    text: 'Export',
                    buttons: [
                        'copy',
                        'excel',
                        'csv',
                        'pdf'
                    ]
                },
                {extend: 'create', editor: editor},
                {extend: 'edit', editor: editor},
                {
                    extend: "selected",
                    text: 'Duplicate',
                    action: function (e, dt, node, config) {
                        // Start in edit mode, and then change to create
                        editor
                            .edit(table.rows({selected: true}).indexes(), {
                                title: 'Duplicate record',
                                buttons: 'Create from existing'
                            })
                            .mode('create');
                    }
                },
                {extend: 'remove', editor: editor},
                {
                    text: 'Import CSV',
                    action: function () {
                        uploadEditor.create({
                            title: 'CSV file import'
                        });
                        $('#tableModal').modal('hide');
                    }
                },
                {
                    extend: 'selectAll',
                    className: 'btn-space'
                },
                {
                    extend: 'colvis',
                    collectionLayout: 'fixed two-column'
                },
            ]
        });

    });
}