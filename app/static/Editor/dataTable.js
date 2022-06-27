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
            $('#modalTable').modal('show');
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

function getColumnIndex(tableElem, colName) {
    var rows = document.getElementById(tableElem).getElementsByTagName('tr');
    for (var j = 0, col; col = rows[0].cells[j]; j++) {
        if (rows[0].cells[j].innerHTML === colName) {
            return j;
        }
    }
}

function onMetricClick(elem, opts, multiple=false) {
    elem.onclick = '';
    let defaultValue = [elem.innerHTML];
    if (multiple) {
        defaultValue = elem.innerHTML.split("|");
        elem.innerHTML = `<select name='metric_select' id='metric_select' multiple></select>`;
    } else {
        elem.innerHTML = `<select name='metric_select' id='metric_select'></select>`;
    }
    let metricSelect = $(`select[name='metric_select']`);
    let metricSelectize = metricSelect.selectize({options: opts,
                                                  searchField: 'text',
                                                  items: defaultValue,
                                                  delimiter: '|',
                                                  onBlur: function() {submitSelection(opts, multiple);}
                                                  });
    metricSelectize[0].selectize.open()
}

function submitSelection(opts, multiple) {
    let value = document.getElementById('metric_select').selectize.getValue();
    if (multiple) {
        value = value.join("|")
    }
    let cellElem = document.getElementById('metric_select').parentElement;
    cellElem.innerHTML = value;
    cellElem.onclick = function() {onMetricClick(this, opts, multiple);}
}

function resetIndex(tableElem) {
    indexColIndex = getColumnIndex(tableElem, 'index');
    var rows = document.getElementById(tableElem).getElementsByTagName('tr');
    for (var i = 1, row; row = rows[i]; i++) {
        let indexElem = rows[i].cells[indexColIndex]
        indexElem.innerHTML = rows[i].rowIndex - 1
    }
}

function addActiveMetric(vmcOptions, rawOptions) {
    let tableName = 'metrics_table'
    let tableElem = tableName + 'Elem'
    let table = document.getElementById(tableName);
    let row = table.insertRow(-1);
    let nameCell = row.insertCell(0);
    let valueCell = row.insertCell(1);
    let indexCell = row.insertCell(2);
    let deleteCell = row.insertCell(3);

    nameCell.innerHTML = document.getElementById('metric_name_select').selectize.getValue();
    nameCell.onclick = function() {onMetricClick(this, vmcOptions);}

    valueCell.innerHTML = document.getElementById('metric_value_select').selectize.getValue().join("|");
    valueCell.onclick = function() {onMetricClick(this, rawOptions, multiple=true);}

    indexCell.innerHTML = row.rowIndex - 1;
    deleteCell.innerHTML = deleteButton(tableName);
    resetIndex(tableElem);
    $('#activeMetricModal').modal('hide');
    document.getElementById('metric_name_select').selectize.clear();
    document.getElementById('metric_value_select').selectize.clear();
}

function deleteTableRow(obj) {
    $(obj).closest("tr").remove()
}

function deleteButton(tableName) {
    let tableElem = tableName + 'Elem'
    let deleteButtonHTML = `<button class="btn btn-danger btn-sm"
        onclick="deleteTableRow(this); resetIndex('${tableElem}');"
        tabindex="0" aria-controls=${tableName} type="button">
          <i class="fas fa-minus" style="color:white"></i>
        </button>`
    return deleteButtonHTML
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
                $('#modalTable').modal('hide');
            }
        });
        editor.on('close', function () {
            if (elem === 'modal-body-table') {
                $('#modalTable').modal('show');
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
                        $('#modalTable').modal('hide');
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

function createMetricTable(colData, rawData, tableName,
                           elem, rawColData, vmcColData) {
    let cols = JSON.parse(colData);
    let rawCols = JSON.parse(rawColData);
    let vmcCols = JSON.parse(vmcColData);

    var vmcOptions = vmcCols.map(function (e) {
        return {text: e, value: e}
    });
    var rawOptions = rawCols.map(function (e) {
        return {text: e, value: e}
    });

    let buttonsHtml = `<button class="btn btn-success" data-toggle="modal" data-target="#activeMetricModal"
        tabindex="0" aria-controls=${tableName} type="button">
          <i class="fas fa-plus" style="color:white"></i>
        </button>`

    let deleteButtonHtml = deleteButton(tableName)
    let tableJquery = '#' + tableName;
    document.getElementById(elem).innerHTML = buttonsHtml + rawData;
    $(document).ready(function () {
        modalElem = $("#activeMetricModal .modal-body")
        modalElem.html(`<div class="form-group row justify-content-center align-items-center">
                          <div class="col-md-4">
                            <label for="metric_name_select">Metric Name</label>
                            <select name='metric_name_select' id='metric_name_select'></select>
                          </div>
                        </div>
                        <div class="form-group row justify-content-center align-items-center">
                          <div class="col-md-4">
                            <label for="metric_value_select">Metric Value</label>
                            <select name='metric_value_select' id='metric_value_select' multiple></select>
                          </div>
                        </div>`);
        let modalSaveButton = $('#activeMetricModalTableSaveButton')
        modalSaveButton.attr("onclick", `addActiveMetric(${JSON.stringify(vmcOptions)}, ${JSON.stringify(rawOptions)})`)
        let nameSelect = $(`select[name='metric_name_select']`);
        let nameSelectize = nameSelect.selectize({options: vmcOptions,
                                                  searchField: 'text',
                                                  delimiter: '|'
                                                  });
        let valueSelect = $(`select[name='metric_value_select']`);
        let valueSelectize = valueSelect.selectize({options: rawOptions,
                                                  searchField: 'text',
                                                  delimiter: '|'
                                                  });

        nameColIndex = getColumnIndex(elem, 'Metric Name');
        valueColIndex = getColumnIndex(elem, 'Metric Value');

        var rows = document.getElementById(elem).getElementsByTagName('tr');
        rows[0].setAttribute('style',"text-align: left");
        for (var i = 1, row; row = rows[i]; i++) {
            rows[i].setAttribute("style", "word-break:break-all")
            nameElem = rows[i].cells[nameColIndex]
            nameElem.onclick = function() {onMetricClick(this, vmcOptions);}

            valueElem = rows[i].cells[valueColIndex]
            valueElem.onclick = function() {onMetricClick(this, rawOptions, multiple=true);}

            let deleteCell = rows[i].insertCell(-1)
            deleteCell.innerHTML = deleteButtonHtml
        }
    });
}

function getMetricTableAsArray() {
    let metricArray = $('#metrics_table tbody').children().map(function() {
        let children = $(this).children();
        return {
            'Metric Name': children.eq(0).text(),
            'Metric Value': children.eq(1).text(),
            'index': children.eq(2).text()
        }
    }).get();
    return metricArray
}

function getTableAsArray(tableId) {
    rows = document.getElementById(tableId).getElementsByTagName('tr');
    let tableArray = []
    for (var i = 1, row; row = rows[i]; i++) {
        col = rows[i].children
        row = {}
        for (var j = 0, col; col = rows[0].cells[j]; j++) {
            let col_name = rows[0].cells[j].textContent
            let row_value = rows[i].cells[j].textContent
            row[col_name] = row_value
        }
        tableArray.push(row)
    }
    return tableArray
}

function createChangeDictOrder(colData, rawData, tableName, dictColData,
                               elem = "change-order-modal-body-table") {
    let dictCols = JSON.parse(dictColData);
    let dictOptions = dictCols.map(function (e) {
        return {text: e, value: e}
    });
    document.getElementById(elem).innerHTML = rawData;
    labelColIndex = getColumnIndex(elem, '');
    let rows = document.getElementById(elem).getElementsByTagName('tr');
    for (var i = 3, row; row = rows[i]; i++) {
        labelElem = rows[i].cells[labelColIndex]
        let defaultValue = [labelElem.innerHTML];
        labelElem.innerHTML = `<select name='auto_order_select${i}' id='auto_order_select${i}'></select>`;
        let labelSelect = $(`select[name='auto_order_select${i}']`);
        let labelSelectize = labelSelect.selectize({options: dictOptions,
                                                    searchField: 'text',
                                                    items: defaultValue,
                                                    delimiter: '|',
                                                    });
        labelSelectize[0].selectize.addOption({value:defaultValue, text:defaultValue});
        labelSelectize[0].selectize.addItem(defaultValue);
    }
}