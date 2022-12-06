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

        nameColIndex = getColumnIndex(elem, 'Metric Name');
        valueColIndex = getColumnIndex(elem, 'Metric Value');

        var rows = document.getElementById(elem).getElementsByTagName('tr');
        rows[0].setAttribute('style',"text-align: left");
        for (var i = 1, row; row = rows[i]; i++) {
            rows[i].setAttribute("style", "word-break:break-word")
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
            let col_name = rows[0].cells[j].textContent
            let row_value = rows[i].cells[j].textContent
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
    let labelColIndex = getColumnIndex(elem, '');
    let rows = document.getElementById(elem).getElementsByTagName('tr');
    rows[0].cells[labelColIndex].innerHTML = 'Auto Dictionary Order'
    for (let i=0, row; row=rows[i]; i++) {
        let buttonCell = rows[i].insertCell(1)
        buttonCell.style.verticalAlign = "middle"
    }
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
    let firstRow = 3
    let rows = document.getElementById(modalElem).getElementsByTagName('tr');
    for (let i = firstRow; i < rows.length; i++) {
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
        getDictColumnDetails(currentSelect.selectize, relationData);
    }
    addMoreOrderOptions(relationData);
}

function shiftOrderDown(modalElem, relationData) {
    let firstRow = 3
    let rows = document.getElementById(modalElem).getElementsByTagName('tr');
    for (let i = rows.length - 1; i >= firstRow; i--) {
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
        getDictColumnDetails(currentSelect.selectize, relationData);
    }
    addMoreOrderOptions(relationData);
}

function parseCombCols(selectElem, relationData) {
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
    if (relationKeys.includes(selectElem.items[0])
        || relationValues.includes(selectElem.items[0])) {
        let key, defaultValue
        if (relationKeys.includes(selectElem.items[0])) {
            key = selectElem.items[0]
            defaultValue = relationAuto[key][0]
        } else {
            key = relationKeys.find(key =>
                relationAuto[key].includes(selectElem.items[0]));
            defaultValue = selectElem.items[0];
            selectElem.setValue(key, true);
        }
        let subOptions = relationAuto[key].map(function (e) {
            return {text: e, value: e}
        });
        if (!document.getElementById(subSelectId)) {
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
    let combKey = ':::'
    let rows = document.getElementById(elem).getElementsByTagName('tr');
    for (let i = 3, row; row = rows[i]; i++) {
        let value = document.getElementById(`auto_order_select${i}`)
            .selectize.getValue();
        if (document.getElementById(`auto_order_select${i}subSelect`)) {
            value = document.getElementById(`auto_order_select${i}subSelect`)
                .selectize.getValue();
        }
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
    let forbiddenDelim = getForbiddenDelim(colName, relationData);
    if (forbiddenDelim) {
        delimOptions = delimOptions.filter(e => e != forbiddenDelim);
    }
    let defaultDelim = delimOptions[0]
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
    let colName = selectElems[0].selectize.getValue();
    let forbiddenDelim = getForbiddenDelim(colName, relationData);
    let leadDelim = getLeadDelim(colName, relationData);
    if (forbiddenDelim) {
        delimOptions = delimOptions.filter(e => e != forbiddenDelim);
    }
    let defaultDelim = delimOptions[0]
    let rows = selectElems.map(function(e) {return e.closest('tr')});
    let indexedRows = rows.filter(e =>
        Boolean(e.getAttribute('data-index')));
    let unindexedRows = rows.filter(e =>
        !Boolean(e.getAttribute('data-index')));
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
    let inds = indexedRows.map(function(e) {
        return e.getAttribute('data-index')
    })
    for (let row of unindexedRows) {
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
    let delimOptions = ['-', '_', '/']
    let moreButton = `<button type="button" class="btn btn-primary btn-circle btn-sm"
                              data-dismiss="modal"
                              data-toggle="modal" data-target="#changeOrderMoreOptions">
                        <i class="fas fa-ellipsis-h" style="color:white"></i>
                      </button>`
    let $primarySelects = $('select[id*=auto_order_select]').not('[id$=subSelect]')
    let $secondarySelects = $('select[id*=auto_order_select][id$=subSelect]')
    let allVals = []
    $primarySelects.each(function(index, elem) {
        allVals.push(elem.value);
        let row = elem.closest("tr");
        row.cells[1].innerHTML = ''
    })
    $secondarySelects.each(function(index, elem) {
        allVals.push(elem.value);
    })
    let uniqueVals = new Set(allVals);
    uniqueVals.delete('mpMisc');
    for (let val of uniqueVals) {
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
            for (let elem of matchingSelects) {
                let row = elem.closest("tr")
                row.removeAttribute('data-index')
                row.removeAttribute('data-delim')
            }
        }
    }
}
