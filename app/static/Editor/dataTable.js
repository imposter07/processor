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

/**
 * Replace element with selectize dropdown populated with opts
 * @param {Element} elem     Element whose innerHtml will be replaced.
 * @param {Array}   opts     Options for the select. [{text: "Example1",
 *     value: "Example1"}].
 * @param {Boolean} multiple Flag for allowing selection of multiple options.
 */
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

/**
 * Replace selectize parent element with text of selected options.
 * @param {Array}   opts     Options for the select. [{text: "Example1",
 *     value: "Example1"}]
 * @param {Boolean} multiple Flag for allowing selection of multiple options.
 */
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

/**
 * Add row to the metrics table using values from the activeMetricModal.
 * @param {Array} vmcOptions Options for the metric name select.
 *     [{text: "Example1" value: "Example1"}]
 * @param {Array} rawOptions Options for the metric value select.
 *     [{text: "Example1" value: "Example1"}]
 */
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

/**
 * Create active metrics table.
 * @param {string} colData    Stringified list of columns in the table.
 * @param {string} rawData    Html string of table element.
 * @param {string} tableName  ID of table element.
 * @param {string} elem       ID of containing element.
 * @param {string} rawColData Stringified list of possible metric value strings.
 * @param {string} vmcColData Stringified list of possible metric name strings.
 */
function createMetricTable(colData, rawData, tableName,
                           elem, rawColData, vmcColData) {
    let cols = JSON.parse(colData);
    let rawCols = JSON.parse(rawColData);
    let vmcCols = JSON.parse(vmcColData);

    let vmcOptions = vmcCols.map(function (e) {
        return {text: e, value: e}
    });
    let rawOptions = rawCols.map(function (e) {
        return {text: e, value: e}
    });

    let buttonsHtml = `<button class="btn btn-success" data-toggle="modal" data-target="#activeMetricModal"
        tabindex="0" aria-controls=${tableName} type="button">
          <i class="fas fa-plus" style="color:white"></i>
        </button>`

    let deleteButtonHtml = deleteButton(tableName)
    document.getElementById(elem).innerHTML = buttonsHtml + rawData;
    $(document).ready(function () {
        // Populate modal with select inputs with relevant options.
        let modalElem = $("#activeMetricModal .modal-body")
        modalElem.html(`<div class="form-group row justify-content-center align-items-center">
                          <div class="col-md-6" style="word-break: break-word">
                            <label for="metric_name_select">Metric Name</label>
                            <select name='metric_name_select' id='metric_name_select'></select>
                          </div>
                        </div>
                        <div class="form-group row justify-content-center align-items-center">
                          <div class="col-md-6" style="word-break: break-word">
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

        // Add onclick events and delete button to each table row.
        let nameColIndex = getColumnIndex(elem, 'Metric Name');
        let valueColIndex = getColumnIndex(elem, 'Metric Value');

        let rows = document.getElementById(elem).getElementsByTagName('tr');
        rows[0].setAttribute('style',"text-align: left");
        for (let i = 1, row; row = rows[i]; i++) {
            rows[i].setAttribute("style", "word-break:break-word")
            let nameElem = rows[i].cells[nameColIndex]
            nameElem.onclick = function() {onMetricClick(this, vmcOptions);}

            let valueElem = rows[i].cells[valueColIndex]
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

function getTableAsArray(tableId, cols=[]) {
    let rows = document.getElementById(tableId).getElementsByTagName('tr');
    let tableArray = []
    for (var i = 1, row; row = rows[i]; i++) {
        if (row.id.includes("Hidden")) {
            continue
        }
        col = rows[i].children
        row = {}
        for (var j = 0, col; col = rows[0].cells[j]; j++) {
            if ((cols.length === 0) || (cols.includes(rows[0].cells[j].innerHTML))) {
                let col_name = rows[0].cells[j].innerText.replace(/(\r\n|\n|\r)/gm, "");
                let row_value = rows[i].cells[j].innerText.replace(/(\r\n|\n|\r)/gm, "");
                row[col_name] = row_value
            }
        }
        tableArray.push(row)
    }
    return tableArray
}

function createChangeDictOrder(colData, rawData, tableName, dictColData,
                               relationalData,
                               elem = "modal-body-table") {
    let dictCols = JSON.parse(dictColData);
    let dictOptions = dictCols.map(function (e) {
        return {text: e, value: e}
    });
    let relationalOrderData = JSON.parse(relationalData);
    // Populate default modal with button and table html
    let buttonsHtml = `<div class="text-left">
        <button class="btn btn-primary" id="shiftUp" tabindex="0"  
            aria-controls=${tableName} type="button" title="Shift order up">
          <i class="fas fa-angle-double-up" style="color:white"></i>
        </button>
        <button class="btn btn-primary" id="shiftDown" tabindex="1" 
            aria-controls=${tableName} type="button" title="Shift order down">
          <i class="fas fa-angle-double-down" style="color:white"></i>
        </button>
      </div>`

    document.getElementById(elem).innerHTML = buttonsHtml + rawData;
    document.getElementById(elem).querySelector('table').classList
        .add('anchor_first_col')
    document.getElementById('shiftDown').onclick = function() {
        shiftOrderDown(elem, relationalOrderData);
    }
    document.getElementById('shiftUp').onclick = function() {
        shiftOrderUp(elem, relationalOrderData);
    }
    // Add empty column to each row for more options button.
    let labelColIndex = getColumnIndex(elem, '');
    let rows = document.getElementById(elem).getElementsByTagName('tr');
    rows[0].cells[labelColIndex].innerHTML = 'Auto Dictionary Order'
    for (let i=0, row; row=rows[i]; i++) {
        let buttonCell = rows[i].insertCell(1)
        buttonCell.style.verticalAlign = "middle"
    }
    // Add select input with auto dictionary columns and event listeners to each row
    for (let i = 3, row; row = rows[i]; i++) {
        let labelElem = rows[i].cells[labelColIndex]
        let defaultValue = labelElem.innerHTML;
        dictOptions.push({text: defaultValue, value: defaultValue})
        labelElem.innerHTML = `<div class='form-group' style='margin-bottom: 0;'>
                                 <select name='auto_order_select${i}' 
                                    id='auto_order_select${i}'></select>
                               </div>`;
        let labelSelect = $(`select[name='auto_order_select${i}']`);
        labelSelect.selectize({options: dictOptions,
                               searchField: 'text',
                               dropdownParent: 'body',
                               items: [defaultValue],
                               delimiter: '|',
                               create: true,
                               persist: false,
                               showAddOptionOnCreate: true,
                               onBlur: function() {if (this.getValue() === '') {
                                   this.setValue('mpMisc');}},
                               onChange: function() {
                                   getDictColumnDetails(this, relationalOrderData);
                                   clearAttributes(this);
                                   addMoreOrderOptions(relationalOrderData);},
                               onInitialize: function() {
                                   parseCombCols(this, relationalOrderData);
                                   getDictColumnDetails(this, relationalOrderData);}
        });
        dictOptions.pop();
    }
    addMoreOrderOptions(relationalOrderData);
}

function shiftOrderUp(modalElem, relationData) {
    // Shift selections in the change order modal up one space.
    let firstRow = 3
    let rows = document.getElementById(modalElem).getElementsByTagName('tr');
    for (let i = firstRow; i < rows.length; i++) {
        // Get the value of the select input in the next row. If there is a
        // subSelect, use that value. If there is no next row, default mpMisc.
        let nextValue
        let currentSelect = document.getElementById(`auto_order_select${i}`);
        if (document.getElementById(`auto_order_select${i+1}subSelect`)) {
            nextValue = $(`#auto_order_select${i+1}subSelect`).val();
        } else {
            nextValue = $(`#auto_order_select${i+1}`).val();
        }
        if (!nextValue) {
            nextValue = 'mpMisc'
        }
        // Shift dataset attributes up one row.
        if (rows[i+1] && rows[i+1].getAttribute('data-index')) {
            rows[i].setAttribute('data-index',
                                 rows[i+1].getAttribute('data-index'));
            rows[i].setAttribute('data-delim',
                                 rows[i+1].getAttribute('data-delim'));
        } else {
            rows[i].removeAttribute('data-index');
            rows[i].removeAttribute('data-delim');
        }
        currentSelect.selectize.addOption({value:nextValue, text:nextValue});
        currentSelect.selectize.setValue(nextValue, true);
        // Add or update subSelect based on new selection.
        getDictColumnDetails(currentSelect.selectize, relationData);
    }
    addMoreOrderOptions(relationData);
}

function shiftOrderDown(modalElem, relationData) {
    // Shift selections in the change order modal up one space.
    let firstRow = 3
    let rows = document.getElementById(modalElem).getElementsByTagName('tr');
    for (let i = rows.length - 1; i >= firstRow; i--) {
        // Get the value of the select input in the previous row. If there is a
        // subSelect, use that value. If there is no next row, default mpMisc.
        let prevValue
        let currentSelect = document.getElementById(`auto_order_select${i}`)
        if (document.getElementById(`auto_order_select${i-1}subSelect`)) {
            prevValue = $(`#auto_order_select${i-1}subSelect`).val();
        } else {
            prevValue = $(`#auto_order_select${i-1}`).val();
        }
        if (!prevValue) {
            prevValue = 'mpMisc'
        }
        // Shift dataset attributes down one row.
        if (rows[i-1].getAttribute('data-index')) {
            rows[i].setAttribute('data-index',
                                 rows[i-1].getAttribute('data-index'));
            rows[i].setAttribute('data-delim',
                                 rows[i-1].getAttribute('data-delim'));
        } else {
            rows[i].removeAttribute('data-index');
            rows[i].removeAttribute('data-delim');
        }
        currentSelect.selectize.addOption({value:prevValue, text:prevValue});
        currentSelect.selectize.setValue(prevValue, true);
        // Add or update subSelect based on new selection.
        getDictColumnDetails(currentSelect.selectize, relationData);
    }
    addMoreOrderOptions(relationData);
}

function parseCombCols(selectElem, relationData) {
    // If the select element has a value of the form
    // <column>:::<index>:::<delimiter>, set the select value to column and
    // add index and delim data attributes to tr element.
    let combKey = ':::'
    let relKeys = Object.keys(relationData[0]);
    let relValues = [].concat(...Object.values(relationData[0]));
    let rowElem = selectElem.$input.closest('tr')[0];
    let initVal = selectElem.getValue();
    let col, index, delim
    if (initVal.includes(combKey)) {
        [col, index, delim] = initVal.split(combKey);
        rowElem.setAttribute('data-index', index);
        rowElem.setAttribute('data-delim', delim);
        if (relValues.includes(col) || !relKeys.includes(col)) {
            selectElem.setValue(col, true);
        }
    }
}

function clearAttributes(selectElem) {
    let rowElem = selectElem.$input.closest('tr')[0];
    rowElem.removeAttribute('data-index');
    rowElem.removeAttribute('data-delim');
}

function getDictColumnDetails(selectElem, relationalData) {
    let relationAuto = relationalData[0]
    let relationKeys = Object.keys(relationAuto);
    let relationValues = [].concat(...Object.values(relationAuto));
    let subSelectId = selectElem.$input[0].id + 'subSelect'
    // If selected option belongs to a relation table, update subSelect.
    // Otherwise, remove any existing subSelect.
    if (relationKeys.includes(selectElem.items[0])
        || relationValues.includes(selectElem.items[0])) {
        let key, defaultValue
        if (relationKeys.includes(selectElem.items[0])) {
            // Use the first Auto value for the selected relation key as the
            // default value of the subSelect.
            key = selectElem.items[0]
            defaultValue = relationAuto[key][0]
        } else {
            // Set the primary select to the relation key, and set the default
            // subSelect value to the original selected value.
            key = relationKeys.find(key =>
                relationAuto[key].includes(selectElem.items[0]));
            defaultValue = selectElem.items[0];
            selectElem.setValue(key, true);
        }
        let subOptions = relationAuto[key].map(function (e) {
            return {text: e, value: e}
        });
        if (!document.getElementById(subSelectId)) {
            // Create subSelect with relevant options for selected relation key.
            let newSelect = document.createElement("select");
            newSelect.id = subSelectId;
            selectElem.$input[0].parentElement.appendChild(newSelect);
            $('#'+ subSelectId).selectize({options: subOptions,
                                           items: [defaultValue],
                                           dropdownParent: 'body',
                                           onBlur: function() {if (this.getValue() === '') {
                                               this.setValue(
                                                   Object.keys(this.options)[0]
                                               )}},
                                           onChange: function() {
                                               clearAttributes(this);
                                               addMoreOrderOptions(relationalData);}
                                           });
        } else {
            // Update existing subSelect value and options.
            let currentSub = document.getElementById(subSelectId);
            if (!relationAuto[key].includes(currentSub.selectize.getValue())) {
                currentSub.selectize.clear(true);
                currentSub.selectize.clearOptions(true);
                currentSub.selectize.addOption(subOptions);
            }
            currentSub.selectize.setValue(defaultValue, true);
        }
    } else if (document.getElementById(subSelectId)) {
        document.getElementById(subSelectId).selectize.destroy();
        document.getElementById(subSelectId).remove();
    }
}

function removeChangeOrderSelectize(elem = "modal-body-table") {
    // Set td innerHTML to equivalent string of all user input for given row.
    let combKey = ':::'
    let rows = document.getElementById(elem).getElementsByTagName('tr');
    for (let i = 3, row; row = rows[i]; i++) {
        // Get primary select value, or subSelect value if one exists.
        let value = document.getElementById(`auto_order_select${i}`)
            .selectize.getValue();
        if (document.getElementById(`auto_order_select${i}subSelect`)) {
            value = document.getElementById(`auto_order_select${i}subSelect`)
                .selectize.getValue();
        }
        // Set text value to <value>:::<index>:::<delimiter> from dataset
        // attributes if applicable.
        if (rows[i].getAttribute('data-index')) {
            let index = rows[i].getAttribute('data-index');
            let delim = rows[i].getAttribute('data-delim');
            value = [value, index, delim].join(combKey)
        }
        let cellElem = document.getElementById(`auto_order_select${i}`).parentElement;
        cellElem.innerHTML = value;
    }
}

function getForbiddenDelim(colName, relationData) {
    // Get delimiter that would shift data to next Auto relation column.
    // i.e. If the Auto column is 'column1::_::column2', then
    // colName=column1 => '_'.
    let forbiddenDelim;
    let [relationAuto, relationDelim] = relationData;
    let relKey = Object.keys(relationAuto).find(
        key => relationAuto[key].includes(colName));
    if (relKey) {
        forbiddenDelim = relationDelim[relKey][
            relationAuto[relKey].findIndex(e => e == colName)];
    }
    return forbiddenDelim
}

function getLeadDelim(colName, relationData) {
    // Get delimiter that precedes the given Auto relation column.
    // i.e. If the Auto column is 'column1::_::column2', then
    // colName=column2 => '_'.
    let leadDelim;
    let [relationAuto, relationDelim] = relationData;
    let relKey = Object.keys(relationAuto).find(
        key => relationAuto[key].includes(colName));
    if (relKey) {
        leadDelim = relationDelim[relKey][
            relationAuto[relKey].findIndex(e => e == colName) - 1];
    }
    return leadDelim
}

function populateMoreOptionsModal(selectElems, relationData, delimOptions) {
    // Populate modal allowing users to further customize dictionary columns
    // which combine multiple strings.
    let modalElem = document.getElementById('changeOrderMoreOptions');
    let colName = selectElems[0].selectize.getValue();
    let leadDelim = getLeadDelim(colName, relationData);
    modalElem.querySelector('#changeOrderMoreOptionsSave').onclick =
        function() {saveMoreOptions(leadDelim)};
    modalElem.getElementsByClassName('modal-title')[0].innerHTML = colName
    modalElem.querySelector('#moreOptionsEditor').innerHTML = `
        <label>Column Order</label>
        <input type="text">
        <label>Column Delimiter</label>
        <select></select>`
    let inputElem = modalElem.querySelector('#moreOptionsEditor input');
    let delimSelect = modalElem.querySelector('#moreOptionsEditor select');
    // Remove any forbidden delimiters from options.
    let forbiddenDelim = getForbiddenDelim(colName, relationData);
    if (forbiddenDelim) {
        delimOptions = delimOptions.filter(e => e != forbiddenDelim);
    }
    let defaultDelim = delimOptions[0]
    // Get sample data from the relevant rows for the user to manipulate.
    let sampleData = selectElems.map(function(e) {
        let row = e.closest('tr');
        let data = row.getElementsByTagName('td')[2].innerHTML
        let index, delim
        if (row.getAttribute('data-index')) {
            index = row.getAttribute('data-index')
            delim = row.getAttribute('data-delim')
            if (delimOptions.includes(delim)) {
                defaultDelim = delim
            }
        }
        return {data: data, index: index, delim: delim, rowIndex: row.rowIndex}
    });
    // Arrange sample data into current order, structure as select options.
    sampleData.sort((a, b) => (Number(a.index) > Number(b.index)) ? 1 : -1);
    let data = sampleData.map(function(e) {
        return {text: e.data, value: e.rowIndex}
    })
    delimOptions = delimOptions.map(function(e) {
        return {text: e, value: e}
    })
    inputElem.value = data.map(function(e) {return e.value}).join(',')
    $(inputElem).selectize({
        options: data,
        plugins: ['drag_drop'],
        delimiter: ',',
    });
    $(delimSelect).selectize({
        options: delimOptions,
        items: [defaultDelim]
    });
    //TODO: Set up preview
}

/**
 * Given an array of integer strings, return the smallest missing integer >= 0.
 * If no integers are missing, return the next integer.
 * @param   {Array}    indexArr Array of integer strings, i.e. ['0', '2']
 * @returns {string}            Minimum missing integer.
 */
function findMinIndex(indexArr) {
    if (!indexArr.length) {
        return String(0)
    }
    let maxInd = Math.max(...indexArr)
    for (let i=0; i <= maxInd + 1; i++) {
        if (!indexArr.includes(String(i))) {
            return String(i)
        }
    }
}

function replaceForbiddenDelim(rows, forbiddenDelim, defaultDelim) {
    for (let row of rows) {
        if (row.getAttribute('data-delim') == forbiddenDelim
                && Number(row.getAttribute('data-index')) > 0) {
            row.setAttribute('data-delim', defaultDelim)
        }
    }
}

function assignIndexDelim(selectElems, relationData, delimOptions) {
    // Set data-index and data-delim for the each tr containing selectElems.
    let colName = selectElems[0].selectize.getValue();
    let forbiddenDelim = getForbiddenDelim(colName, relationData);
    let leadDelim = getLeadDelim(colName, relationData);
    if (forbiddenDelim) {
        delimOptions = delimOptions.filter(e => e != forbiddenDelim);
    }
    let defaultDelim = delimOptions[0]
    // Get associated tr for each selectElems. Split into indexed and unindexed.
    let rows = selectElems.map(function(e) {return e.closest('tr')});
    let indexedRows = rows.filter(e =>
        Boolean(e.getAttribute('data-index')));
    let unindexedRows = rows.filter(e =>
        !Boolean(e.getAttribute('data-index')));
    // Use previously selected delimiter for these elements, if applicable.
    if (indexedRows.length) {
        for (let row of indexedRows) {
            let index = row.getAttribute('data-index');
            let delim = row.getAttribute('data-delim');
            if (Number(index) > 0 && delimOptions.includes(delim)) {
                defaultDelim = delim;
                break;
            }
        }
    }
    // Get list of indexes already assigned.
    let inds = indexedRows.map(function(e) {
        return e.getAttribute('data-index')
    })
    for (let row of unindexedRows) {
        // Assign unindexed row smallest available index and required delimiter.
        let minInd = findMinIndex(inds);
        inds.push(minInd);
        row.setAttribute('data-index', minInd);
        if (Number(minInd) > 0 || !leadDelim) {
            row.setAttribute('data-delim', defaultDelim);
        } else {
            row.setAttribute('data-delim', leadDelim);
        }
    }
}

function saveMoreOptions(leadDelim) {
    // Update tr data-index and data-delim attributes to reflect user input.
    let modalElem = document.getElementById('moreOptionsEditor');
    let inputElem = modalElem.getElementsByTagName('input')[0];
    let selectElem = modalElem.getElementsByTagName('select')[0];
    let rowOrder = inputElem.selectize.getValue().split(',');
    let delim = selectElem.selectize.getValue();
    let index = 0;
    let rows = document.getElementById('modal-body-table')
        .getElementsByTagName('tr');
    for (let rowIndex of rowOrder) {
        rows[rowIndex].setAttribute('data-index', index);
        if (index > 0 || !leadDelim) {
            rows[rowIndex].setAttribute('data-delim', delim);
        } else {
            rows[rowIndex].setAttribute('data-delim', leadDelim);
        }
        index++
    }
}

function addMoreOrderOptions(relationData) {
    // Add more options button to any tr where at least one other tr has the
    // same selected option.
    let delimOptions = ['-', '_', '/']
    let moreButton = `<button type="button" class="btn btn-primary btn-circle btn-sm"
                              data-dismiss="modal"
                              data-toggle="modal" data-target="#changeOrderMoreOptions">
                        <i class="fas fa-ellipsis-h" style="color:white"></i>
                      </button>`
    let $primarySelects = $('select[id*=auto_order_select]').not('[id$=subSelect]')
    let $secondarySelects = $('select[id*=auto_order_select][id$=subSelect]')
    let allVals = []
    // Populate allVals with the value of all select inputs in the auto order
    // column. Remove the more options button from each row.
    $primarySelects.each(function(index, elem) {
        allVals.push(elem.value);
        let row = elem.closest("tr");
        row.cells[1].innerHTML = ''
    })
    $secondarySelects.each(function(index, elem) {
        allVals.push(elem.value);
    })
    // Get all unique options currently in use.
    let uniqueVals = new Set(allVals);
    uniqueVals.delete('mpMisc');
    for (let val of uniqueVals) {
        // Get list of all select inputs with given value (val).
        let matchingSelects = []
        $primarySelects.each(function(index, elem) {
            if (elem.value === val) {
                let subSelectId = elem.id + 'subSelect'
                if (!document.getElementById(subSelectId)) {
                    matchingSelects.push(elem)
                }
            }
        });
        $secondarySelects.each(function(index, elem) {
            if (elem.value === val) {
                matchingSelects.push(elem)
            }
        });
        // If there is more than one select input with the same value, set
        // index and delimiter attributes for tr and add more options button.
        if (matchingSelects.length > 1) {
            assignIndexDelim(matchingSelects, relationData, delimOptions)
            for (let elem of matchingSelects) {
                let row = elem.closest("tr")
                row.cells[1].innerHTML = moreButton
                row.getElementsByTagName('button')[0].onclick = function () {
                    populateMoreOptionsModal(matchingSelects, relationData,
                                             delimOptions)
                }
            }
        } else {
            // Remove unnecessary index and delimiter information for unique
            // selections.
            for (let elem of matchingSelects) {
                let row = elem.closest("tr")
                row.removeAttribute('data-index')
                row.removeAttribute('data-delim')
            }
        }
    }
}
