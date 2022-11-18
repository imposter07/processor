function createTableElements(tableName, rowsName,
                             topRowsName = '', tableTitle = '',
                             tableDescription = '', colToggle = '',
                             tableAccordion = '', specifyFormCols = '') {
    let collapseStr = (tableAccordion) ? 'collapse' : '';
    let title = (tableTitle) ? `
        <div class="card-header">
            <h5>${tableTitle}</h5>
            <h6 class="card-subtitle mb-2 text-muted">
                ${tableDescription}
            </h6>
        </div>`: '';
    let colToggleHtml = (colToggle) ? `
        <div class="row">
            <div class="col" style="display:none;">
                <div id="selectColumnsToggle${tableName}">
                    Select Columns...
                </div>
            </div>
        </div>`: '';
    let colToggleBtnHtml = (colToggle) ? `
        <button id="toggleMetrics"
                class="btn btn-outline-secondary text-left"
                type="button" href="" onclick="toggleMetricsSelect('${tableName}');">
            <i class="fas fa-list-check"
               role="button"></i>
            Toggle Metrics
        </button>
    `: '';
    let topRowsBtnHtml = (topRowsName) ? `
        <button id="addTopRow${tableName}"
                class="btn btn-outline-success text-left"
                type="button" href="">
            <i class="fas fa-plus"
               role="button"></i>
            Add ${topRowsName}
        </button>
    ` : '';
    let elem = document.getElementById(tableName);
    let elemToAdd = `
    <div class="card shadow outer text-center">
        ${title}
    <div class="card-body">
        ${colToggleHtml}
        <div class="row">
            <div id="${tableName}TableSlideCol" class="col-xs"
                 style="" data-value="${topRowsName}">
                <div id="${tableName}TableSlide" class="card-deck"></div>
            </div>
            <div class="col">
                <div class="row">
                    <div class="col">
                        <div id="addRowsPlaceholder${tableName}" class="input-group mb-3">
                        </div>
                    </div>
                    <div class="col">
                        <div class="btn-group">
                            ${topRowsBtnHtml}
                            ${colToggleBtnHtml}
                        </div>
                    </div>
                    <div class="col">
                        <div class="input-group mb-3">
                            <div class="input-group-prepend">
                                <span class="input-group-text"><i
                                        class="fa-solid fa-magnifying-glass"
                                        href="#"
                                        role="button"></i></span>
                            </div>
                            <input id="tableSearchInput" type="text"
                                   class="form-control"
                                   placeholder="Search"
                                   aria-label="Username"
                                   aria-describedby="basic-addon1"
                                   onkeyup="searchTable('#${tableName}Table')">
                        </div>
                    </div>
                </div>
                <table id="${tableName}Table" data-value="${rowsName}" data-accordion="${collapseStr}"
                       data-specifyform="${specifyFormCols}"
                       class="table table-striped table-responsive-sm small"></table>
            </div>
        </div>
    </div>
    `
    elem.insertAdjacentHTML('beforeend', elemToAdd);
}

function addDays(date, days) {
    let result = new Date(date);
    result.setDate(result.getDate() + days);
    return result
}

function shadeDates(loopIndex, dateRange = null, cellClass = "shadeCell") {
    if (!dateRange) {
        const fp = document.querySelector('#datePicker' + loopIndex)._flatpickr;
        dateRange = fp.selectedDates;
    }
    let startDate = new Date(dateRange[0]);
    let endDate = new Date(dateRange[1]);
    let weekString =  '';
    let calStart = '';
    let calEnd = '';
    let element = '';
    let elementId = '';
    let weeks = document.querySelectorAll('*[id^="col20"]');
    weeks.forEach(week => {
        weekString = week.id.replace('col', '');
        calStart = new Date(weekString);
        calEnd = addDays(calStart, 6);
        elementId = "row" + weekString + loopIndex;
        if (calEnd >= startDate && calStart <= endDate) {
            element = document.getElementById(elementId);
            element.classList.add(cellClass);
        }
        else {
            element = document.getElementById(elementId);
            element.classList.remove(cellClass);
        }
    });
}

function getRowHtml(loopIndex, tableName) {
    let tableHeadElems = document.getElementById(tableName + 'TableHeader');
    tableHeadElems = Array.from(tableHeadElems.getElementsByTagName('th'));
    let tableHeaders = '';
    tableHeadElems.forEach(tableHeadElem => {
        tableHeaders = tableHeaders + `<td id="row${tableHeadElem.id.replace('col', '')}${loopIndex}" style="display:${tableHeadElem.style.display};"></td>`
    });
    return tableHeaders
}

function buildFormFromCols(loopIndex, formNames, tableName) {
    let fromFromCols = '';
    formNames.forEach((formName) => {
        let colName = 'col' + formName;
        let colType = document.getElementById(colName).dataset['type'];
        let inputCheck = colType === 'select';
        let inputStartHtml = (inputCheck) ? '<select ' : '<input type="text" value="" step="any"'
        let inputInnerHtml = (inputCheck) ? document.getElementById('colSelect' + formName).innerHTML : '';
        let inputEndHtml = (inputCheck) ? '</select>' : '';
        let inputIdHtml = (inputCheck) ? formName.toLowerCase() + 'Select' + loopIndex : formName + loopIndex;
        let topRowIds = getTopRowIds(tableName);
        let topRowData = '';
        topRowIds.forEach(topRowId => {
            topRowData += ' data-' + topRowId + '=""'
        });
        let displayColNames = generateDisplayColumnName(formName);
        fromFromCols += `
        <div class="col-4 form-group" id="${formName}FormGroupCol">
            <label class="control-label" for="${inputIdHtml}">${displayColNames}</label>
                   ${inputStartHtml} class="form-control form-control-sm"
                   id="${inputIdHtml}" name="${inputIdHtml}"
                   onchange="syncSingleTableWithForm(${loopIndex}, '${formName}', '${tableName}')"
                   oninput="syncSingleTableWithForm(${loopIndex}, '${formName}', '${tableName}')"
                   ${topRowData}>
            ${inputInnerHtml}${inputEndHtml}
        </div>`
    });
    return fromFromCols
}

function getTableHeadElems(tableName) {
    let tableHeadElems = document.getElementById(tableName + 'TableHeader');
    tableHeadElems = Array.from(tableHeadElems.getElementsByTagName('th'));
    return tableHeadElems
}

function findInQuerySelectorAll(findName, selectorIdVal, selectorPrefix = '') {
    let selector = `${selectorPrefix}[id^='${selectorIdVal}']`;
    let selectorElems = document.querySelectorAll(selector);
    let selectorElem = Array.from(selectorElems).find(elem => {return elem.innerHTML === findName});
    if (selectorElem){
        return selectorElem.id.replace(selectorIdVal, '');
    }
}

function getRowFormNames(tableName){
    let tableHeadElems = getTableHeadElems(tableName);
    return tableHeadElems.filter(col => col.dataset['form'] === 'true').map(col => col.id.replace('col', ''));
}

function addRowToTable(rowData, tableName) {
    let curTable = document.getElementById(tableName + 'Table');
    let bodyId = tableName + 'Body';
    let d1 = document.getElementById(bodyId);
    if (!(d1)) {
        curTable.innerHTML += `<tbody id="${bodyId}"></tbody>`;
    }
    d1 = document.getElementById(bodyId);
    let formHolderName = tableName + 'FormHolder';
    let loopIndex = d1.querySelectorAll(`.${formHolderName}:last-child`);
    loopIndex = (loopIndex.length != 0) ? (parseInt(loopIndex[loopIndex.length- 1].id.replace(formHolderName, '')) + 1).toString() : '0'
    let tableHeadElems = getTableHeadElems(tableName);
    let tableHeaders = getRowHtml(loopIndex, tableName);
    let rowFormNames = getRowFormNames(tableName);
    let rowForm = buildFormFromCols(loopIndex, rowFormNames, tableName);
    let hiddenRowHtml = `
        <tr id="trHidden${loopIndex}">
            <td colspan="${tableHeadElems.length}" class="hiddenRow">
            <div id="collapseRow${loopIndex}" class="collapse"
                 aria-labelledby="heading${loopIndex}"
                 data-parent="#${bodyId}">
                <div class="card-body">
                    <div class="${formHolderName}" id="${formHolderName}${loopIndex}" >
                        <form id="form${tableName}${loopIndex}"  class="row">
                            <div class="col form-group">
                                <label class="control-label" for="datePicker${loopIndex}">Dates</label>
                                <input id="datePicker${loopIndex}"
                                       class="custom-select custom-select-sm
                                                              flatpickr flatpickr-input active"
                                       type="text" placeholder="Date"
                                       data-id="range" name="dates${loopIndex}"
                                       readonly="readonly" data-input>
                            </div>
                            <div class="col-4 form-group">
                                <label class="control-label" for="deleteRow${loopIndex}">DELETE</label>
                                <button id="deleteRow${loopIndex}" onclick="deleteRow(${loopIndex}, ${tableName});"
                                        class="btn btn-block btn-outline-danger text-left" type="button" href="">
                                    <i class="fas fa-trash" role="button" aria-hidden="true"></i>
                                    DELETE
                                </button>
                            </div>
                            ${rowForm}
                        </form>
                    </div>
                </div>
            </div>
            </td>
        </tr>`;
    let collapseStr = curTable.getAttribute('data-accordion');
    let rowCard = `
        <tr id="tr${loopIndex}" data-toggle="${collapseStr}"
            data-target="#collapseRow${loopIndex}" class="accordion-toggle">
            ${tableHeaders}
        </tr>
        ${hiddenRowHtml}`;
    d1.insertAdjacentHTML('beforeend', rowCard);
    addSelectize();
    addDatePicker();
    addOnClickEvent('[id^=topRowHeader]', editTopRowOnClick);
    sortTable(bodyId, tableName + 'TableHeader');
    return loopIndex
}

function addRowDetailsToForm(rowData, loopIndex, tableName) {
    let topRowElem = '';
    let topRowsName = document.getElementById(tableName + 'TableSlideCol').getAttribute('data-value');
    if (topRowsName in rowData) {
        let topRowCurName = rowData[topRowsName];
        let topRowIndex = findInQuerySelectorAll(topRowCurName, 'row' + topRowsName);
        topRowElem = document.getElementById('topRowHeader' + tableName + topRowIndex);
        topRowElem.click();
    }
    let fp = document.getElementById("datePicker" + loopIndex)._flatpickr;
    fp.setDate([rowData.start_date, rowData.end_date]);
    let rowFormNames = getRowFormNames(tableName);
    rowFormNames.forEach((rowFormName) => {
        let colName = 'col' + rowFormName;
        let colElem = document.getElementById(colName);
        let inputCheck = colElem.dataset['type'] === 'select';
        let currentElemId = (inputCheck) ? rowFormName.toLowerCase() + 'Select' + loopIndex : rowFormName + loopIndex;
        let curElem = document.getElementById(currentElemId);
        let name = colElem.getAttribute('data-name');
        let newValue = rowData[name];
        if (inputCheck) {
            curElem.selectize.addOption({
                text: newValue,
                id: newValue,
                value: newValue
            });
            curElem.selectize.setValue(newValue);
        } else {
            curElem.value = newValue;
        }
    });
    syncTableWithForm(loopIndex, rowFormNames, tableName);
    shadeDates(loopIndex, null, 'shadeCell' + loopIndex);
    if (topRowElem) {
        topRowElem.click();
    }
}

function deleteRow(loopIndex, tableName) {
    document.getElementById('tr' + loopIndex).remove();
    document.getElementById('trHidden' + loopIndex).remove();
    populateTotalCards(tableName);
}

function addRow(rowData = null, tableName) {
    if (rowData) {
        let rowName = document.getElementById(`${tableName}Table`).getAttribute('data-value');
        let loopIndex = findInQuerySelectorAll(rowData[rowName.toLowerCase()], `row${rowName}`);
        if (!loopIndex) {
            loopIndex = addRowToTable(rowData, tableName);
        }
        addRowDetailsToForm(rowData, loopIndex, tableName);
    }
    else {
        addRowToTable(rowData, tableName);
    }
    toggleMetrics(tableName);
}

function addRowsOnClick() {
    let tableName = this.id.replace('addRows', '');
    loadingBtn(this, this.style, this.class);
    let rowName = document.getElementById(`${tableName}Table`).getAttribute('data-value');
    let rowNameLower = rowName.toLowerCase();
    let selectAddId = `${rowNameLower}SelectAdd`;
    let newRowNames = document.getElementById(selectAddId).selectize.getValue();
    document.getElementById(selectAddId).selectize.clear();
    let newRowsData = newRowNames.map(function (e) {
        let metricElem = document.getElementById(`colSelect${rowName}`).querySelector('option[value="' + e + '"]');
        let metrics = (metricElem === null) ? {cpc: 0, cpm: 0} : metricElem.dataset
        return Object.assign({}, {name: e, total_budget: 0,
            start_date: "{{ object.start_date }}", end_date: "{{ object.end_date }}"},
            metrics)
    });
    newRowsData.forEach(newRowData => {
        addRow(newRowData, tableName, );
    });
    addElemRemoveLoadingBtn(this.id);
}

function addTopRowCard(tableName, loopIndex, topRowData = null) {
    let topRowName = document.getElementById(tableName + 'TableSlideCol').getAttribute('data-value');
    let formNames = [topRowName, 'total_budget'];
    let topRowCardId = 'topRowCard' + tableName + loopIndex;
    let topRowFormId = 'topRowForm' + tableName + loopIndex;
    let topRowForm = buildFormFromCols(loopIndex, formNames, tableName);
    let topRowCard = `
        <div id="${topRowCardId}"
            class="card shadow outer text-center" style="display: none;">
            <div class="card-header">

            </div>
            <div class="card-body">
                <form id="${topRowFormId}"  class="">
                    ${topRowForm}
                    <div class="col form-group">
                        <label class="control-label" for="datePicker` + loopIndex + `">Dates</label>
                        <input id="datePicker` + loopIndex + `"
                               class="custom-select custom-select-sm
                                                      flatpickr flatpickr-input active"
                               type="text" placeholder="Date"
                               data-id="range" name="dates` + loopIndex + `"
                               readonly="readonly" data-input>
                    </div>
                </form>
            </div>
        </div>
    `
    let elem = document.getElementById(tableName + 'TableSlide');
    elem.insertAdjacentHTML('beforeend', topRowCard);
    toggleTopRowCard(tableName, loopIndex);
    addOnClickEvent('[id^=topRowHeader]', editTopRowOnClick);
    addSelectize();
    addDatePicker();
    let topRowNameLower = topRowName.toLowerCase();
    if (topRowData) {
        let selectize = $(`#${topRowNameLower}Select` + loopIndex)[0].selectize;
        selectize.addOption({
            text: topRowData.name,
            id: topRowData.name,
            value: topRowData.name
        });
        selectize.setValue(topRowData.name);
        let fp = document.getElementById("datePicker" + loopIndex)._flatpickr;
        fp.setDate([topRowData.start_date, topRowData.end_date]);
    }
}

function turnOffAllCards(tableName) {
    document.getElementById(tableName + 'TableSlideCol').style.display = 'none';
    let allCards = document.querySelectorAll(`[id^='topRowCard${tableName}']`);
    allCards.forEach(card => {
        card.style.display = 'none'
    });
    let topRowElems = document.querySelectorAll(`[id^='topRowHeader${tableName}']`);
    topRowElems.forEach(elem => {
        elem.classList.remove('shadeCell');
    });
}

function setTopRowMetrics(currentIndex, tableName) {
    let tableHeadElems = getTableHeadElems(tableName);
    tableHeadElems.forEach(col => {
        if (col.dataset['form'] === 'true' && col.dataset['type'] !== 'select') {
            let colName = col.id.replace('col', '');
            let elems = document.querySelectorAll(`input[id^=${colName}]`);
            elems.forEach(e => {
                if (e.id.replace(/\d+/g, '') !== colName ) {
                    return;
                }
                let loopIndex = e.id.replace(colName, '');
                let newVal = null;
                if (currentIndex) {
                    if (e.dataset[currentIndex]) {
                        newVal = e.dataset[currentIndex];
                    }
                }
                else {
                    newVal = 0;
                    if (!document.getElementById('col' + colName).dataset['type'].includes('metrics')) {
                        Object.entries(e.dataset).forEach(x => newVal += parseFloat(x[1]));
                    }
                    else {
                        let costElem = document.getElementById('total_budget' + loopIndex);
                        Object.entries(costElem.dataset).forEach(x => newVal += parseFloat(x[1]));
                        let calcVal = 0;
                        Object.entries(costElem.dataset).forEach(x => {
                            let calcElem = parseFloat(e.dataset[x[0]])
                            let costElem = parseFloat(x[1])
                            calcVal += (costElem / calcElem)
                        })
                        newVal = newVal / calcVal;
                        newVal = newVal.toFixed(2)
                        /*
                        if (colName === 'cpm') {
                            newVal = newVal * 1000;
                        }*/
                    }
                }
                if (newVal){
                    e.value = newVal;
                    syncSingleTableWithForm(loopIndex, colName, tableName, true);
                }
            })
        }
    })
}

function toggleTopRowCard(tableName, currentIndex) {
    let cardId = 'topRowCard' + tableName + currentIndex;
    let slideColId = tableName + 'TableSlideCol';
    let slideCol = document.getElementById(slideColId);
    let currentCard = document.getElementById(cardId);
    if (currentCard.style.display === 'none') {
        turnOffAllCards(tableName);
        currentCard.style.display = '';
        slideCol.style.display = '';
        document.getElementById('topRowHeader' + tableName + currentIndex).classList.add('shadeCell');
        setTopRowMetrics(currentIndex, tableName);
    }
    else {
        turnOffAllCards(tableName);
        setTopRowMetrics(null, tableName);
    }
}

function toggleMetrics(tableName) {
    let selElem = document.getElementById(`selectColumns${tableName}`);
    if (!(selElem)) return;
    let selectize = selElem.selectize;
    let availOptions = Object.keys(selectize.options)
    let selected = selectize.getValue();
    availOptions.forEach(o => {
        let elems = document.querySelectorAll("[id*='" + o + "']");
        let display = (selected.includes(o)) ? "" : "none";
        elems.forEach(e => {
            e.style.display = display;
        })
    })
}

function toggleMetricsOnChange() {
    let tableName = this.id.replace('selectColumnsToggle', '');
    toggleMetrics(tableName);
}

function toggleMetricsSelect(tableName) {
    let elem = document.getElementById('selectColumnsToggle' + tableName);
    let clickElem = document.getElementById('toggleMetrics');
    if (elem.parentElement.style.display === "none") {
        elem.parentElement.style.display = "";
        clickElem.classList.remove('btn-outline-secondary');
        clickElem.classList.add('btn-secondary');
    }
    else {
        elem.parentElement.style.display = "none";
        clickElem.classList.add('btn-outline-secondary');
        clickElem.classList.remove('btn-secondary');
    }
}

function editTopRowOnClick() {
    let currentIndex = '-' + this.id.replace(/\D/g, "");
    let tableName = this.id.replace('topRowHeader', '').replace(currentIndex, '');
    toggleTopRowCard(tableName, currentIndex);
}

function addTopRowOnClick() {
    loadingBtn(this, '', this.class);
    let topRowsName = '';
    let tableName = this.id.replace('addTopRow', '')
    addTopRow({'name': `NEW ${topRowsName}`}, tableName);
    addElemRemoveLoadingBtn(this.id);
}

function addDatePicker() {
    let selector = '[id^=datePicker]';
    $(selector).flatpickr({
        mode: "range",
    });
    $(selector).change(function () {
        let loopIndex = this.id.replace('datePicker', '');
        let shadeColor = loopIndex.replace('-', '')
        shadeDates(loopIndex, null, 'shadeCell' + shadeColor);
    });
}

function addRows(rows, tableName) {
    rows.forEach(row => {
        addRow(row, tableName);
    });
}

function syncSingleTableWithForm(loopIndex, formName, tableName, topRowToggle = false) {
    let colName = 'col' + formName;
    let curColElem = document.getElementById(colName);
    let inputCheck = curColElem.dataset['type'] === 'select';
    let currentElemId = (inputCheck) ? formName.toLowerCase() + 'Select' + loopIndex : formName + loopIndex;
    let currentElem = document.getElementById(currentElemId);
    let currentValue = (inputCheck) ? currentElem.selectize.getValue() : currentElem.value;
    if (!inputCheck && !topRowToggle) {
        let topRowElemIdSelector = 'topRowHeader' + tableName;
        let selectedElem = document.querySelectorAll(`[id^='${topRowElemIdSelector}'].shadeCell`);
        if (selectedElem.length !== 0) {
            let topRowId = selectedElem[0].id.replace(topRowElemIdSelector, '');
            currentElem.dataset[topRowId] = currentElem.value;
        }
        else {
            let topRowIds = getTopRowIds(tableName);
            let topRowVal = currentElem.value;
            if (!curColElem.dataset['type'].includes('metrics')) {
                topRowVal = topRowVal / topRowIds.length;
            }
            topRowIds.forEach(topRowId => {
                currentElem.dataset[topRowId] = topRowVal;
            })
        }
    }
    document.getElementById('row' + formName + loopIndex).innerHTML = currentValue;
    let curRow = document.getElementById('tr' + loopIndex);
    let blankHighlight = curColElem.getAttribute('data-blank_highlight');
    if ((curRow) && (blankHighlight)) {
        if (document.getElementById('row' + currentElemId).innerHTML === "$0") {
            document.getElementById('tr' + loopIndex).classList.add('shadeCellError');
        } else {
            document.getElementById('tr' + loopIndex).classList.remove('shadeCellError');
        }
    }
    populateTotalCards(tableName);
}

function syncTableWithForm(loopIndex, formNames, tableName) {
    formNames.forEach((formName) => {
        syncSingleTableWithForm(loopIndex, formName, tableName);
    });
}

function formatNumber(currentNumber) {
    return currentNumber.toFixed(0).toLocaleString("en-US").replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function createTotalCards(tableName) {
    let elem = document.getElementById(tableName);
    elem.insertAdjacentHTML('afterbegin',`
        <div class="card shadow outer text-center">
            <div class="card-header">
                <h5>Metric Totals</h5>
                <h6 class="card-subtitle mb-2 text-muted">Total metric values
                    for the current plan.</h6>
            </div>
            <div class="card-body">
                <div class="card-deck container-fluid text-center">
                    <div id="${tableName}TotalCards" class="row w-100"></div>
                </div>
            </div>
        </div>`
    )
}

function populateTotalCards(tableName) {
    if (!document.getElementById(tableName + 'TotalCards')) return;
    let table = document.getElementById(tableName + 'Body');
    let formatter = new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
    });
    let formNames = [
        ['', 'total_budget', 1],
        ['cpm', 'Impressions', 0],
        ['cpc', 'Clicks', 0],
        ['cplpv', 'Landing Page', 0],
        ['cpbc', 'Button Clicks', 0],
        ['cpv', 'Views', 0],
        ['cpcv', 'Video Views 100', 0]]
    let data = [];
    data = formNames.map(function (e) {
        return {name: e[1], numeric_value: 0, current_value: 0, msg: '', change: ''}
    })
    if (table) {
        let rowName = document.getElementById(`${tableName}Table`).getAttribute('data-value');
        let currentRows = document.querySelectorAll(`[id^='row${rowName}']`);
        currentRows.forEach(currentRow => {
            let rowNum = currentRow.id.replace(`row${rowName}`, '');
            if (rowNum[0] !== '-') {
                let rowCost = parseFloat(document.getElementById('rowtotal_budget' + rowNum).innerHTML.replace('$', ''));
                formNames.forEach(formName => {
                    let costPerName = formName[0];
                    let summableName = formName[1];
                    let targetColName = formName[formName[2]];
                    let rowValue = parseFloat(document.getElementById('row' + targetColName + rowNum).innerHTML.replace('$', ''));
                    let idx = data.findIndex(x => x.name === summableName);
                    if (costPerName !== '') {
                        rowValue = rowCost / rowValue;
                        if (summableName === 'Impressions') {
                            rowValue = rowValue * 1000;
                        }
                        document.getElementById('row' + summableName + rowNum).innerHTML = formatNumber(rowValue);
                    }
                    data[idx]['numeric_value'] += rowValue;
                });
            }
        })
    }
    let idxSumCost = data.findIndex(x => x.name === 'total_budget');
    let sumCost = data[idxSumCost]['numeric_value'];
    formNames.forEach(formName => {
        let costPerName = formName[0];
        let summableName = formName[1];
        let idx = data.findIndex(x => x.name === summableName);
        data[idx]['current_value'] = formatNumber(data[idx]['numeric_value']);
        if (costPerName !== '') {
            let curVal = sumCost / data[idx]['numeric_value'];
            if (summableName === 'Impressions') {
                curVal = curVal * 1000;
            }
            data.push({name: costPerName, numeric_value: curVal, current_value: formatter.format(curVal)})
        }
        else {
            data[idx]['msg'] = "Of Total Budget";
            data[idx]['change'] = sumCost / parseFloat("{{ object.total_budget }}");
        }
    });
    generateTotalCards(tableName + 'TotalCards', data);
    toggleMetrics(tableName);
}

function getTopRowIds(tableName) {
    let topRowIdSelector = 'topRowHeader' + tableName;
    let topRowElems = document.querySelectorAll(`[id^="${topRowIdSelector}"]`);
    return Array.prototype.map.call(topRowElems, function (elem) {
        return parseInt(elem.id.replace(topRowIdSelector, ''));
    });
}

function addDataToFormForNewTopRow(tableName, topRowId) {
    let formNames = getRowFormNames(tableName);
    formNames.forEach(formName => {
        if (document.getElementById('col' + formName).dataset['type'].includes('metrics') || formName === 'total_budget') {
            let elems = document.querySelectorAll(`input[id^=${formName}]`);
            elems.forEach(e => {
                if (!e.dataset[topRowId]) {
                    let newVal = (formName === 'total_budget') ? 0 : e.dataset[topRowId - 1];
                    e.dataset[topRowId] = newVal;
                }
            })
        }
    })
}

function addTopRow(topRowData, tableName) {
    let thead = document.getElementById(tableName + 'TableTHead');
    let rowName = document.getElementById(`${tableName}Table`).getAttribute('data-value');
    let tHeadIds = getTopRowIds(tableName);
    let minId = (tHeadIds.length !== 0) ? (Math.min.apply(Math, tHeadIds) - 1).toString() : '-1';
    let minIdSelector = `row${rowName}${minId}`;
    let colorClass = "shadeCell" + minId.replace('-', '');
    let tableHeaders = getRowHtml(minId, tableName);
    let topRow = `<tr id="topRowHeader${tableName}${minId}">${tableHeaders}</tr>`;
    thead.innerHTML = topRow + thead.innerHTML;
    let firstCell = document.getElementById(minIdSelector);
    firstCell.classList.add('text-uppercase');
    firstCell.classList.add('font-weight-bold');
    document.getElementById(minIdSelector).innerHTML = topRowData.name;
    shadeDates(minId, [topRowData.start_date, topRowData.end_date], colorClass);
    addTopRowCard(tableName, minId, topRowData);
    addDataToFormForNewTopRow(tableName, minId);
}

function addCurrentTopRows(topRowsData, tableName) {
    topRowsData.forEach(function (topRowData, i) {
        addTopRow(topRowData, tableName);
    });
    turnOffAllCards(tableName);
}

function generateDisplayColumnName(colName) {
    return colName.toUpperCase().replace('ESTIMATED_', 'e').split('_').join(' ');
}

function addTableColumns(cols, name) {
    let tHeadName =  name + 'TableTHead';
    let table = document.getElementById(name + 'Table');
    table.innerHTML += `<thead id ="${tHeadName}"><tr id="${name}TableHeader"></tr></thead>`;
    let thead = document.getElementById(name + 'TableHeader');
    let selectColsElem = document.getElementById('selectColumnsToggle' + name);
    if (selectColsElem) {
        selectColsElem.innerHTML = `
            <select id="selectColumns${name}" multiple="" class="width100 form-control">
                <option value="">Select Columns To Add...</option>
            </select>`;
        addSelectize();
        addOnClickEvent('[id^="selectColumnsToggle"]', toggleMetricsOnChange, 'change');
    }
    let specifyFormCol = table.getAttribute('data-specifyform');
    cols.forEach(col => {
        let colName = col['name'];
        let formCol = (!(specifyFormCol)) ? 'true': col['form'];
        let blankHighlight = existsInJson(col, 'blank_highlight');
        thead.innerHTML += `
            <th data-form="${formCol}" data-type="${col['type']}"
                data-blank_highlight="${blankHighlight}" data-name="${colName}"
                id="col${colName}">${generateDisplayColumnName(colName)}
            </th>`;
        if (col['hidden']) {
            document.getElementById('col' + colName).style.display = 'none';
        }
        if (col['type'] === 'select') {
            let selectName = 'colSelect' + colName;
            let colElem = document.getElementById('col' + colName);
            colElem.innerHTML += `<select id="` + selectName + `" hidden=''></select>`;
            let colSelectElem = document.getElementById(selectName);
            col['values'].forEach(val => {
                let optionData = '';
                Object.entries(val).forEach(([k,v]) => {
                    optionData += `data-` + k + `="` + v +`" `;
                })
                colSelectElem.innerHTML += `
                    <option ` + optionData + `
                            value="` + val[colName] + `">` + val[colName] + `</option>`;
            })
            if (col['add_select_box']) {
                let elem = document.getElementById('addRowsPlaceholder' + name);
                let placeHolderName = 'selectAdd' + colName + 'Placeholder' + name;
                let newElem = `
                    <div id="${placeHolderName}">Select ${colName}...</div>
                    <div class="input-group-append">
                        <button id="addRows${name}"
                                class="btn btn-outline-success btn-block text-left"
                                type="button" href="">
                            <i class="fas fa-plus"  role="button"></i>
                        </button>
                    </div>`;
                elem.insertAdjacentHTML('beforeend', newElem);
                let addBoxName = colName.toLowerCase() + 'SelectAdd';
                document.getElementById(placeHolderName).innerHTML = `
                <select id="${addBoxName}" multiple="" class="width100 form-control">
                    <option value="">Select ${colName}s To Add...</option>
                </select>`;
                document.getElementById(addBoxName).innerHTML += document.getElementById(selectName).innerHTML;
            }
            addSelectize();
        }
        if (col['type'].includes('metrics')) {
            let selectize = document.getElementById(`selectColumns${name}`).selectize;
            selectize.addOption({
                text: generateDisplayColumnName(colName),
                id: colName,
                value: colName
            });
            if (col['type'] === 'default_metrics') {
                let curVal = selectize.getValue();
                curVal.push(colName);
                selectize.setValue(curVal);
            }
        }
    });
}

function convertColsToObject(cols) {
    return cols.map(x => {
        return ({name: x, type: '', add_select_box: false,
            hidden: false, header: false, form: false});
    });
}

function createLiquidTable(data, kwargs) {
    let tableName = kwargs['tableName'];
    let tableData = data['data'];
    let topRowsName = existsInJson(tableData, 'top_rows_name');
    let rowsName = existsInJson(tableData, 'rows_name');
    let title = existsInJson(tableData, 'title');
    let description = existsInJson(tableData, 'description');
    let colToggle = existsInJson(tableData, 'columns_toggle');
    let totalCards = existsInJson(tableData, 'totals');
    let tableRows = existsInJson(tableData, 'data');
    let tableTopRows = existsInJson(tableData, 'top_rows');
    let tableCols = existsInJson(tableData, 'cols');
    let tableAccordion = existsInJson(tableData, 'accordion');
    let specifyFormCols = existsInJson(tableData, 'specify_form_cols');
    let colDict = existsInJson(tableData, 'col_dict');
    if (!(colDict)) {
        tableCols = convertColsToObject(tableCols);
    }
    createTableElements(tableName, rowsName, topRowsName, title,
        description, colToggle, tableAccordion, specifyFormCols);
    addTableColumns(tableCols, tableName);
    if (topRowsName) {
        addCurrentTopRows(tableTopRows, tableName);
    }
    if (tableRows) {
        addRows(tableRows, tableName);
    }
    if (totalCards) {
        createTotalCards(tableName);
        populateTotalCards(tableName);
    }
    addSelectize();
    addDatePicker();
    addOnClickEvent('button[id^=addRows]', addRowsOnClick);
    addOnClickEvent('button[id^=addTopRow]', addTopRowOnClick);
}
