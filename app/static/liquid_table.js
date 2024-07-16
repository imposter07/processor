function createModal(modalId='', modalTitleText='', form = null,
                     saveBtnFunction='', saveBtnFunctionKwargs = {}) {
    // Create the modal element
    const modal = document.createElement("div");
    modal.classList.add("modal", "fade");
    modal.setAttribute("tabindex", "-1");
    modal.setAttribute("role", "dialog");
    modal.setAttribute("id", modalId);

    // Create the modal dialog
    const modalDialog = document.createElement("div");
    modalDialog.classList.add("modal-dialog");
    modalDialog.setAttribute("role", "document");
    modal.appendChild(modalDialog);

    // Create the modal content
    const modalContent = document.createElement("div");
    modalContent.classList.add("modal-content");
    modalDialog.appendChild(modalContent);

    // Create the modal header
    const modalHeader = document.createElement("div");
    modalHeader.classList.add("modal-header");
    modalContent.appendChild(modalHeader);

    // Create the modal title
    const modalTitle = document.createElement("h5");
    modalTitle.classList.add("modal-title");
    modalTitle.textContent = modalTitleText;
    modalHeader.appendChild(modalTitle);

    // Create the modal body
    const modalBody = document.createElement("div");
    modalBody.classList.add("modal-body");
    modalContent.appendChild(modalBody);

    if (form) {
        modalBody.appendChild(form);
    }

    // Create the modal footer
    const modalFooter = document.createElement("div");
    modalFooter.classList.add("modal-footer");
    modalContent.appendChild(modalFooter);

    saveBtnFunction = (saveBtnFunction) ? (saveBtnFunction) : function() {};

    // Create the save button
    const saveButton = document.createElement("button");
    saveButton.classList.add("btn", "btn-outline-success", "w-100");
    saveButton.textContent = "Save";
    saveButton.addEventListener("click", function () {
        saveBtnFunction(saveBtnFunctionKwargs);
        document.body.removeChild(modal);
        document.querySelector(".modal-backdrop").remove();
        document.querySelector("body").classList.remove('modal-open');
    });
    modalFooter.appendChild(saveButton);

    // Append the modal to the body
    document.body.appendChild(modal);

    // Display the modal
    let modalElem = $("#" + modalId);
    modalElem.modal("show");

    // When the modal is closed, remove it from the body and return the form to its original location
    modalElem.on("hidden.bs.modal", function() {
        saveBtnFunction(saveBtnFunctionKwargs);
        document.body.removeChild(modal);
    });
    return modal
}

function addNewRowSaveBtnFunction(kwargs) {
    let tableName = kwargs['tableName'];
    let loopIndex = kwargs['loopIndex'];
    let form = document.getElementById(`form${tableName}${loopIndex}`);
    let fh = document.getElementById(`${tableName}FormHolder${loopIndex}`);
    fh.appendChild(form);
}

function addNewRowModal(tableName) {
    let loopIndex = addRow(null, tableName);
    let form  = document.getElementById(`form${tableName}${loopIndex}`);
    let kwargs = {'tableName': tableName, 'loopIndex': loopIndex}
    createModal('addRowModal', 'Add Row', form,
        addNewRowSaveBtnFunction, kwargs);
}

function createSingleButton(tableName, buttonObject) {
    let buttonElem = document.createElement('button');
    buttonElem.dataset['table_name'] = tableName;
    let {classList, content, icon, ...rest} = buttonObject;
    if (!classList) {
        classList = ['btn', 'btn-outline-primary'];
    }
    if (content) {
        let textSpan = document.createElement('span');
        textSpan.textContent = content;
        buttonElem.appendChild(textSpan);
    }
    if (icon) {
        let iconElem = document.createElement('i');
        let {classList, left, ...rest} = icon;
        iconElem.classList.add(...classList);
        setAttributes(iconElem, rest);
        buttonElem.appendChild(iconElem);
        if (left && content) {
            let textSpan = buttonElem.querySelector('span');
            buttonElem.appendChild(textSpan);
        }
    }
    buttonElem.classList.add(...classList);
    setAttributes(buttonElem, rest);
    return buttonElem
}

function createButtonsFromArray(tableName, buttonArray) {
    let buttonElems = document.createDocumentFragment();
    if (!buttonArray) {
        return buttonElems;
    }
    buttonArray.forEach((buttonObject) => {
        let buttonElem = createSingleButton(tableName, buttonObject);
        buttonElems.appendChild(buttonElem);
    });
    return buttonElems
}

function createTableElements(tableName, rowsName,
                             topRowsName = '', tableTitle = '',
                             tableDescription = '', colToggle = '',
                             tableAccordion = '', specifyFormCols = '',
                             rowOnClick = '', newModalBtn = '',
                             colFilter = '', searchBar='',
                             chartBtn='', tableButtons = '') {
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
                class="btn btn-outline-secondary text-start"
                type="button" href="" onclick="toggleMetricsSelect('${tableName}');">
            <i class="bi bi-list-check"
               role="button"></i>
            Toggle Metrics
        </button>
    `: '';
    let topRowsBtnHtml = (topRowsName) ? `
        <button id="addTopRow${tableName}"
                class="btn btn-outline-success text-start"
                type="button" href="">
            <i class="bi bi-plus-lg"
               role="button"></i>
            Add ${topRowsName}
        </button>
    ` : '';
    let newModalBtnHtml = (newModalBtn)? `
        <button id="addNewRowModal${tableName}"
                class="btn btn-outline-success text-start"
                type="button" href=""
                onclick="addNewRowModal('${tableName}');">
            <i class="bi bi-plus-lg"
               role="button"></i>
            Add New Row
        </button>
    ` : '';
    let showSearchBarHtml = (searchBar) ? `
        <div class="input-group-prepend">
                <span class="input-group-text"><i
                        class="bi bi-search"
                        href="#"
                        role="button"></i></span>
            </div>
            <input id="tableSearchInput${tableName}Table" type="text"
                   class="form-control"
                   placeholder="Search"
                   aria-label=""
                   aria-describedby="basic-addon1"
                   onkeyup="searchTable('#${tableName}Table')">
        </div>` : '';
    let showChartBtnHtml = (chartBtn) ? `
            <button id="showChartBtn${tableName}"
                class="btn btn-outline-success text-start d-block"" type="button" 
                href="" onclick="showChart('${tableName}');">
                <i class="bi bi-bar-chart-fill" role="button"></i>
            </button>
    ` : '';
    let customButtons = createButtonsFromArray(tableName, tableButtons);
    let elem = document.getElementById(tableName);
    let elemToAdd = `
    <div class="card shadow outer text-center">
        ${title}
    <div class="card-body">
        ${colToggleHtml}
        <div class="row mb-3">
            <div class="col d-flex">
                <div class="btn-group">
                    ${showChartBtnHtml}
                    ${topRowsBtnHtml}
                    ${colToggleBtnHtml}
                    ${newModalBtnHtml}
                </div>
            </div>
            <div class="col">
                <div id="addRowsPlaceholder${tableName}" class="input-group ">
                </div>
            </div>
            <div class="col">
                <div class="input-group ">
                    ${showSearchBarHtml}
                </div>
            </div>
        </div>
        <div class="row m-2">
            <div id="${tableName}TableSlideCol" class="col"
                 style="display:none;" data-value="${topRowsName}">
                <div id="${tableName}TableSlide" class="card-group"></div>
            </div>
            <div id="${tableName}TableBaseCol" class="col">
                <table id="${tableName}Table" data-value="${rowsName}" data-accordion="${collapseStr}"
                       data-specifyform="${specifyFormCols}" data-rowclick="${rowOnClick}"
                       data-colfilter="${colFilter}" data-totalrows="-1"
                       class="table table-striped table-responsive-sm small"></table>
            </div>
            <div id="${tableName}ChartCol" class="col" style="display: none">
                <div id="${tableName}ChartPlaceholder"></div>
            </div>
        </div>
    </div>
    `
    elem.insertAdjacentHTML('beforeend', elemToAdd);
    elem.querySelector('div.btn-group').appendChild(customButtons);
}

function addDays(date, days) {
    let result = new Date(date);
    result.setDate(result.getDate() + days);
    return result
}

function shadeDates(loopIndex, dateRange = null, cellClass = "shadeCell", tableName='') {
    if (!dateRange) {
        const elem = document.getElementById('datePicker' + loopIndex);
        if (elem) {
            if (elem._flatpickr) {
                dateRange = elem._flatpickr.selectedDates;
            } else {
                dateRange = [
                    elem.getAttribute('data-start-date'),
                    elem.getAttribute('data-end-date')]
            }
        } else {
            return false
        }
    }
    let startDate = new Date(dateRange[0]);
    let endDate = new Date(dateRange[1]);
    let weekString =  '';
    let calStart = '';
    let calEnd = '';
    let element = '';
    let elementId = '';
    let tableHead = document.getElementById(`${tableName}TableHeader`);
    let weeks = tableHead.querySelectorAll('*[id^="col20"]');
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
    ['start_date', 'end_date'].forEach(elemName => {
        let elemId = `row${elemName}${loopIndex}`;
        let elem = document.getElementById(elemId);
        if (elem) {
            let newVal = (elemName === 'start_date') ? startDate : endDate;
            if (!(newVal instanceof Date) || isNaN(newVal.getTime())) {
                newVal = new Date();
            }
            newVal = newVal.toISOString().substring(0, 10);
            elem.textContent = newVal;
        }
    })
}

function liquidTableToObject(tableName) {
    let tableId = `${tableName}Table`;
    let table = document.getElementById(tableId);
    let cols = getTableHeadElems(tableName);
    let rows = table.querySelectorAll("tr:not([id*='Hidden']):not([id*='Header'])");
    let tableDict = {};
    rows.forEach(row => {
        let cells = row.cells;
        for (let j = 0; j < cells.length; j++) {
            let colName = cols[j].dataset['name'];
            if (!(colName in tableDict)) {
                tableDict[colName] = [];
            }
            let cell = cells[j];
            let value = cell.textContent || cell.innerText;
            tableDict[colName].push(value);
        }
    });
    return tableDict
}

function saveLiquidTable(formContinue, tableName) {
    let newPage = '';
    let oldPage = '';
    let listTabs = document.getElementById('listTabs');
    if (listTabs) {
        let btnPrimary = listTabs.querySelector('.btn-primary');
        let nextBtn = btnPrimary.nextElementSibling;
        btnPrimary = btnPrimary.href.split('/');
        oldPage = btnPrimary[btnPrimary.length - 1];
        nextBtn = nextBtn.href.split('/');
        newPage = nextBtn[nextBtn.length - 1];
    }
    let formTopline = {};
    let tableId = `${tableName}Table`
    let topRowIdPrefix = 'topRowHeader' + tableName;
    let topRowsName = document.getElementById(tableName + 'TableSlideCol').getAttribute('data-value');
    let rowName = document.getElementById(`${tableId}`).dataset['value'];
    let topRowElems = document.querySelectorAll(`[id^="${topRowIdPrefix}"]`);
    topRowsName = (topRowsName === 'null') ? "topRows" : topRowsName;
    rowName = (rowName === 'null') ? "rows" : rowName;
    if (topRowElems.length === 0) {
        formTopline[0] = {rowName: {}}
        formTopline[0][rowName] = liquidTableToObject(tableName);
    }
    Array.prototype.map.call(topRowElems, function (elem) {
        elem.click();
        let curIdx = elem.id.replace(topRowIdPrefix, '');
        formTopline[curIdx] = {topRowsName: {}, rowName: {}}
        formTopline[curIdx][topRowsName] = $(`[id^=topRowForm${tableName}${curIdx}]`).serializeArray();
        formTopline[curIdx][rowName] = $(`[id^=form${tableName}]`).serializeArray();
        elem.click();
    });
    SendDataTable(tableName, formContinue, oldPage, newPage, formTopline);
}

function returnBaseFormId() {
    let baseFormId = 'base_form_id';
    let curBaseForm = document.getElementById(baseFormId);
    curBaseForm.id = curBaseForm.dataset['originalId'];
    document.getElementById('base_form_id_original').id = baseFormId;
}

function switchBaseFormId(tableName, loopIndex) {
    let baseFormId = 'base_form_id';
    document.getElementById(baseFormId).id = 'base_form_id_original';
    let newFormId = `form${tableName}${loopIndex}`;
    let newForm = document.getElementById(newFormId);
    newForm.dataset['originalId'] = newForm.id;
    document.getElementById(newFormId).id = baseFormId;
}

function buttonColOnClick(tableName, colName, loopIndex) {
    let form = document.createElement("form");
    form.setAttribute('method',"post");
    let i = document.getElementById(`${colName}${loopIndex}`).parentElement;

    let s = document.createElement("input"); //input element, File
    s.setAttribute('type',"file");
    s.classList.add('filepond');
    form.appendChild(i);
    form.appendChild(s);

    let modalId = `modal${tableName}${colName}${loopIndex}`.toUpperCase();
    let titleName = `${tableName} - ${colName} - ${loopIndex}`;
    switchBaseFormId(tableName, loopIndex);
    createModal(modalId, titleName, form, returnBaseFormId);
    loadJQuery();
    addFilePond();
    addFilePondMeta();
}

function goToUrlFromLink(data, kwargs) {
    window.open(data['url']);
}

function getLink(objectName, viewFunction) {
    let data = {'object_name': objectName, 'view_function': viewFunction}
    makeRequest('url_from_view_function', 'POST', data, goToUrlFromLink);
}

function cellPickOnClick(clickedCell) {
    let highlightStr = 'shadeCell0';
    let cells = clickedCell.parentNode.cells;
    Array.from(cells).forEach(function(cell) {
        cell.classList.remove(highlightStr);
    });
    clickedCell.classList.add(highlightStr);
    let elemId = clickedCell.dataset['target'];
    document.getElementById(elemId).innerText = clickedCell.innerText;
    populateTotalCards(clickedCell.dataset['table']);
}

function getRowHtml(loopIndex, tableName, rowData = null) {
    let tableHeadElems = document.getElementById(tableName + 'TableHeader');
    tableHeadElems = Array.from(tableHeadElems.getElementsByTagName('th'));
    let tableHeaders = '';
    tableHeadElems.forEach(tableHeadElem => {
        let colName = tableHeadElem.id.replace('col', '');
        let cellData = getCellContent(rowData, colName);
        let isButtonCol = tableHeadElem.dataset['type'] === 'button_col';
        let buttonColBtn = `
            <button id="btn${colName}${loopIndex}" class="btn btn-outline-success text-start"
                    type="button" href="" 
                    onclick="buttonColOnClick('${tableName}', '${colName}', '${loopIndex}');">
                <i class="bi bi-plus-lg"  role="button"></i>
            </button>`
        let buttonColHtml = (isButtonCol) ? buttonColBtn: '';
        let isLinkCol = tableHeadElem.dataset['type'] === 'link_col';
        let linkColLink = (isLinkCol) ? tableHeadElem.dataset['link'] : '';
        let linkColHtmlPre = `<a href="javascript:void(0);" onclick="getLink('${cellData}', '${linkColLink}');">`;
        let linkColHtmlPost = `</a>`;
        linkColHtmlPre = (isLinkCol) ? linkColHtmlPre : '';
        linkColHtmlPost = (isLinkCol) ? linkColHtmlPost : '';
        let isCellPickCol = tableHeadElem.dataset['type'] === 'cell_pick_col';
        const cellPickHtml = `onclick="cellPickOnClick(this)" data-target="rowcell_pick_col${loopIndex}" data-table="${tableName}" `;
        let cellPickCol = (isCellPickCol) ? cellPickHtml : '';
        tableHeaders += `
            <td id="row${colName}${loopIndex}" style="display:${tableHeadElem.style.display};" ${cellPickCol}>
                ${linkColHtmlPre}
                    ${cellData}${buttonColHtml}
                ${linkColHtmlPost}
            </td>`
    });
    return tableHeaders
}

function getCellContent(rowData, colName) {
    let cellContent = '';
    if (rowData && rowData[colName] && rowData[colName] !== 'None') {
        cellContent = rowData[colName];
        if (typeof cellContent === 'object') {
            cellContent = JSON.stringify(cellContent);
        }
    }
    return cellContent
}

function getDateForm(loopIndex, tableName) {
    let dateFormGroup = document.createElement('div');
    dateFormGroup.className = 'col mb-3';
    let label = document.createElement('label');
    label.className = 'control-label';
    label.setAttribute('for', `datePicker${loopIndex}`);
    label.textContent = 'Dates';
    let input = document.createElement('input');
    input.id = `datePicker${loopIndex}`;
    input.className = 'form-control form-control-sm flatpickr flatpickr-input active';
    input.type = 'text';
    input.placeholder = 'Date';
    input.setAttribute('data-id', 'range');
    input.setAttribute('data-table', tableName);
    input.name = `dates${loopIndex}`;
    input.readOnly = true;
    input.setAttribute('data-input', '');
    dateFormGroup.appendChild(label);
    dateFormGroup.appendChild(input);
    return dateFormGroup;
}

function changeSliderValues(slider, newValue) {
    slider.value = newValue;
    slider.nextElementSibling.value = newValue;
    let dataCellId = slider.dataset['datacell'];
    let dataCell = document.getElementById(dataCellId);
    let absVal = getTotalValueForSlider(dataCellId) * (newValue / 100.0)
    slider.nextElementSibling.nextElementSibling.value = absVal.toFixed(2);
    let data = JSON.parse(dataCell.value);
    let key = slider.dataset['key'];
    data[key] = newValue / 100.0;
    dataCell.value = JSON.stringify(data);
    let event = new Event('change');
    dataCell.dispatchEvent(event);
}

function adjustOtherSliders(changedSlider) {
    let total = 0;
    let sliders = changedSlider.parentElement.parentElement.querySelectorAll(".slider-value");
    let lockedSliders = 0;
    let lockButtonId = changedSlider.id.replace('Slider', 'Lock');
    let lockButton = document.getElementById(lockButtonId);
    sliders.forEach(slider => {
        total += Number(slider.value);
        lockButtonId = slider.id.replace('Value', 'Lock');
        lockButton = document.getElementById(lockButtonId);
        if (lockButton.textContent === "Unlock") {
            lockedSliders += 1;
        }
    });
    let difference = total - 100;
    let change = difference / (sliders.length - 1 - lockedSliders);
    sliders.forEach(slider => {
        lockButtonId = slider.id.replace('Value', 'Lock');
        lockButton = document.getElementById(lockButtonId);
        let sliderId = slider.id.replace('Value', 'Slider');
        if (sliderId !== changedSlider.id && lockButton.textContent !== "Unlock") {
            let newValue = slider.value - change;
            if (newValue < 0) {
                let changeSliderNewValue = changedSlider.value + newValue;
                changeSliderValues(changedSlider, changeSliderNewValue);
                newValue = 0;
            }
            let actualSlider = document.getElementById(sliderId);
            changeSliderValues(actualSlider, newValue);
        }
    });
}

function syncSliderDataCell(elem, dataCell) {
    let parentElement = elem.parentElement.parentElement;
    let keys = parentElement.querySelectorAll('select.slider-key');
    let values = parentElement.querySelectorAll('.slider-value');
    let sliders = parentElement.querySelectorAll('.slider');
    let data = {};
    keys.forEach((k, idx) => {
        data[k.value] = values[idx].value / 100.0;
        values[idx].dataset['key'] = k.value;
        sliders[idx].dataset['key'] = k.value;
    });
    dataCell.value = JSON.stringify(data);
    let event = new Event('change');
    dataCell.dispatchEvent(event);
}

function sliderKeyEditOnInput(e) {
    let dataCellId = e.target.dataset['datacell'];
    let dataCell = document.getElementById(dataCellId);
    syncSliderDataCell(e.target.parentElement, dataCell);
}

function sliderValueEditOnInput() {
    let sliderValueInput = this;
    let sliderValueId = sliderValueInput.id;
    let totalVal = getTotalValueForSlider(sliderValueInput.dataset['datacell']);
    let newVal = sliderValueInput.value;
    if (sliderValueInput.classList.contains('slider-absolute')) {
        sliderValueId = sliderValueId.replace('Absolute', '');
        sliderValueInput = document.getElementById(sliderValueId);
        newVal = (this.value / totalVal) * 100;
        sliderValueInput.value =  newVal;
    }
    let progressBarId = sliderValueId.replace('Value', 'Slider');
    let progressBar = document.getElementById(progressBarId);
    let lockButtonId = progressBar.id.replace('Slider', 'Lock');
    let lockButton = document.getElementById(lockButtonId);
    if (lockButton.textContent === "Unlock") {
        this.value = sliderValueInput.value;
        return;
    }
    changeSliderValues(progressBar, newVal);
    adjustOtherSliders(progressBar, newVal);
}

function sliderEditOnInput() {
    let progressBar = this;
    let sliderValueInputId = progressBar.id.replace('Slider', 'Value');
    let sliderValueInput = document.getElementById(sliderValueInputId);
    let lockButtonId = progressBar.id.replace('Slider', 'Lock');
    let lockButton = document.getElementById(lockButtonId);
    if (lockButton.textContent === "Unlock") {
        this.value = sliderValueInput.value;
        return;
    }
    changeSliderValues(progressBar, this.value);
    adjustOtherSliders(this, this.value);
}

function toggleOutlineButton(elem, selectStr='', unselectStr='',
                             btnColor='primary') {
    let outlineClass = `btn-outline-${btnColor}`;
    let fillClass = outlineClass.replace('outline-', '');
    if (elem.textContent === selectStr) {
        elem.textContent = unselectStr;
        elem.classList.remove(outlineClass);
        elem.classList.add(fillClass);
    } else {
        elem.textContent = selectStr;
        elem.classList.remove(fillClass);
        elem.classList.add(outlineClass);
    }
}

function lockButtonOnClick() {
    toggleOutlineButton(this, 'Lock', 'Unlock');
}

function deleteSlider(buttonElement) {
    const sliderContainer = buttonElement.parentElement;
    let containerParent = sliderContainer.parentElement;
    sliderContainer.remove();
    syncSliderDataCell(containerParent, containerParent.parentElement.children[1]);
}

function addNewSliderRow(addElem) {
    let key = 'NEW';
    let data = {key: 0}
    let inputElemId = addElem.parentElement.parentElement.children[1].id;
    let newElem = generateSliderContent(key, inputElemId, data);
    addElem.insertAdjacentHTML('beforebegin', newElem);
    addSelectize();
    addOnClickForSlider();
    let elem = addElem.parentElement.querySelectorAll('input.slider')[0];
    let dataCell = elem.dataset['datacell'];
    dataCell = document.getElementById(dataCell);
    syncSliderDataCell(elem, dataCell);
}

function getTotalValueForSlider(inputElemId) {
    let totalElemId = inputElemId.replace('rule_info', 'rowtotal_budget');
    let totalElem = document.getElementById(totalElemId);
    return totalElem.innerHTML.trim();
}

function generateSliderContent(key, inputElemId, data, idx) {
    if (!(idx)) {
        let inputElem = document.getElementById(inputElemId);
        idx = inputElem.parentElement.querySelectorAll('.slider').length + 1;
    }
    let totalVal = getTotalValueForSlider(inputElemId);
    return `
            <div class="col">
            <div class="col">
                <select id="${inputElemId}${idx}SliderKey" class="slider-key" data-datacell="${inputElemId}">
                    <option>${key}</option>
                </select>
            </div>
            <input id="${inputElemId}${key}Slider" class="slider" 
                data-key="${key}" data-datacell="${inputElemId}"
                type="range" step="1" min="0" max="100" value="${data[key] * 100}">
            <input id="${inputElemId}${key}Value" class="slider-value"
                data-key="${key}" data-datacell="${inputElemId}" style="display;"
                type="number" min="0" max="100" step="any" value="${data[key] * 100}">
            <input id="${inputElemId}${key}ValueAbsolute" class="slider-absolute"
                data-key="${key}" data-datacell="${inputElemId}" style="display:none;"
                type="number" min="0" max="${totalVal}" step="any" value="${data[key] * totalVal}">
            <button id="${inputElemId}${key}Lock" class="lock-button btn btn-outline-primary">Lock</button>
            <button onclick="deleteSlider(this)" class="lock-button btn btn-outline-danger">Delete</button>
            </div>
        `;
}

function addOnClickForSlider() {
    addOnClickEvent('.slider', sliderEditOnInput, 'input');
    addOnClickEvent('.slider-value', sliderValueEditOnInput, 'change', false);
    addOnClickEvent('.slider-absolute', sliderValueEditOnInput, 'change', false);
    addOnClickEvent('.slider-key', sliderKeyEditOnInput, 'change', false);
    addOnClickEvent('.lock-button', lockButtonOnClick);
}

function toggleSliderFormat(elem) {
    toggleOutlineButton(elem, 'Switch Dollars', 'Switch Percent', 'success');
    let elemClasses = ['.slider-value', '.slider-absolute'];
    elemClasses.forEach(elemClass => {
        let elems = elem.parentElement.querySelectorAll(elemClass);
        let display = (elems[0].style.display === 'none') ? '': 'none';
        elems.forEach(sliderElem => {
            sliderElem.style.display = display;
        });
    });
}

function lookupValOnChange(e) {
    let containerElem = e.currentTarget.parentElement;
    let selElems = containerElem.querySelectorAll('select');
    selElems.forEach(selElem => {
        let curVals = selElem.selectize.getValue();
        let ruleElemId = selElem.dataset['rule'];
        let ruleInfoElem = document.getElementById(ruleElemId);
        let ruleData = JSON.parse(ruleInfoElem.innerHTML);
        ruleData[selElem.dataset['col']][selElem.dataset['key']] = curVals;
        ruleInfoElem.innerHTML = JSON.stringify(ruleData);
    });
}

function buildLookupContainerElems(ruleInfo, curIdx, placeSearch, lookupContainerId, addVals=false) {
    let lookupContainer = document.getElementById(lookupContainerId);
    lookupContainer.innerHTML = '';
    let containerHtml = '';
    let data = {};
    let idx = 0;
    let ruleInfoId = `rowrule_info${curIdx}`;
    Object.entries(ruleInfo).forEach(rule => {
        let vals = (addVals) ? rule[1] : [];
        data[rule[0]] = vals;
        let optHtml = '';
        vals.forEach(val => {
            optHtml += `<option selected>${val}</option>`;
        });
        containerHtml += `<label>${rule[0]}</label>`;
        containerHtml += `<select data-key="${rule[0]}" data-rule="${ruleInfoId}" data-col="${placeSearch}"
            id="${lookupContainerId}${idx}" multiple="">${optHtml}</select>`;
        idx += 1;
    });
    lookupContainer.insertAdjacentHTML('beforeend', containerHtml);
    addSelectize(`#${lookupContainerId} select`);
    addOnClickEvent(`#${lookupContainerId} select`, lookupValOnChange, 'change', false);
    return [ruleInfoId, data]
}

function lookupColOnChange(e) {
    let lookupCol = e.currentTarget;
    let curIdx = lookupCol.id.replace('rule_info', '').replace('LookupCol', '');
    let partElemId = `rowPartner${curIdx}`;
    let partElem = document.getElementById(partElemId);
    let curPart = partElem.innerHTML.trim();
    let placeSearch = lookupCol.value;
    let placeCols = document.querySelectorAll('*[id^="rowplace_col"]');
    let loopIndex = '';
    placeCols.forEach(placeCol => {
        let val = placeCol.innerHTML.trim();
        if (val === placeSearch) {
            let partId = placeCol.id.replace('place_col', 'Partner');
            let part = document.getElementById(partId).innerHTML.trim();
            if (part === curPart) {
                loopIndex = placeCol.id.replace('rowplace_col', '');
            }
        }
    });
    let ruleInfo = document.getElementById(`rowrule_info${loopIndex}`);
    ruleInfo = JSON.parse(ruleInfo.innerHTML);
    let lookupContainerId = lookupCol.id.replace('Col', 'Container');
    let [ruleInfoId, data] = buildLookupContainerElems(ruleInfo, curIdx, placeSearch, lookupContainerId);
    let ruleInfoElem = document.getElementById(ruleInfoId);
    let ruleData = {};
    ruleData[placeSearch] = data;
    ruleInfoElem.innerHTML = JSON.stringify(ruleData);
}

function buildLookupEditCol(elem, inputElemId, data, isLookup) {
    let selectedCol = '';
    if (isLookup) {
        selectedCol = Object.keys(data)[0];
    }
    let lookupHtml = `<div id="${inputElemId}Lookup" style="display:none;">`;
    let colNames = ['budget', 'country', 'targeting_bucket',
        'creative_line_item', 'copy', 'retailer', 'buy_model', 'buy_rate',
        'start_date', 'serving', 'ad_rate', 'reporting_rate', 'kpi',
        'data_type_1', 'service_fee_rate', 'verification_rate',
        'reporting_source', 'environment', 'size', 'ad_type', 'placement_description',
        'package_description', 'media_channel'];
    let optHtml = '';
    colNames.forEach(colName => {
        let selected = (colName === selectedCol) ? 'selected' : '';
        optHtml += `<option ${selected}>${colName}</option>`;
    });
    let lookupColId = `${inputElemId}LookupCol`;
    let lookupContainerId = `${inputElemId}LookupContainer`;
    lookupHtml += `<select id="${lookupColId}" data-datacell="${inputElemId}">${optHtml}</select>`;
    lookupHtml += `<div id="${lookupContainerId}"></div></div>`;
    elem.insertAdjacentHTML('beforeend', lookupHtml);
    addSelectize(`#${lookupColId}`);
    addOnClickEvent(`#${lookupColId}`, lookupColOnChange, 'change', false);
    if (isLookup) {
        data = data[selectedCol];
        let curIdx = inputElemId.replace('rule_info', '');
        buildLookupContainerElems(data, curIdx, selectedCol, lookupContainerId, true);
    }
}

function switchRuleType() {
    let typeCell = this;
    let curType = typeCell.innerHTML;
    let colId = typeCell.id.replace('rowtype', 'rule_info');
    let turnOnId = colId + curType;
    let turnOnElem = document.getElementById(turnOnId);
    turnOnElem.style.display = '';
    let turnOffId = (curType === 'Create') ? 'Lookup' : 'Create';
    turnOffId = colId + turnOffId;
    let turnOffElem = document.getElementById(turnOffId);
    turnOffElem.style.display = 'none';
}

function buildSliderEditCol(elem, newValue, inputElemId) {
    let data = JSON.parse(newValue);
    let typeElemId = inputElemId.replace('rule_info', 'rowtype');
    let typeElem = document.getElementById(typeElemId);
    let isLookup = typeElem.innerHTML.trim() === 'Lookup';
    let progHtml = `<br><div id="${inputElemId}Create">`;
    let idx = 0
    for (let key in data) {
        let sliderContent = generateSliderContent(key, inputElemId, data, idx);
        progHtml += sliderContent;
        idx += 1;
    }
    progHtml += `<div id="${inputElemId}AddRow" class="btn btn-outline-success" onclick="addNewSliderRow(this)">Add New Row</div>`;
    progHtml += `<div id="${inputElemId}SwitchDollars" class="btn btn-outline-success" onclick="toggleSliderFormat(this)">Switch Dollars</div></div>`;
    elem.insertAdjacentHTML('beforeend', progHtml);
    addSelectize();
    addOnClickForSlider();
    buildLookupEditCol(elem, inputElemId, data, isLookup);
    addOnClickEvent(`#${typeElemId}`, switchRuleType, 'DOMSubtreeModified', false);
    return progHtml
}

function buildFormFromCols(loopIndex, formNames, tableName) {
    let formElements = [];
    let table = document.getElementById(tableName + 'TableHeader');
    let dateCols = ['start_date', 'end_date'];
    let weekColsExist = table.querySelectorAll('*[id^="col20"]').length !== 0;
    if (checkIfExists(formNames, dateCols) || weekColsExist) {
        let dateFormElements = getDateForm(loopIndex, tableName);
        formElements = formElements.concat(dateFormElements);
        formNames = removeValues(formNames, dateCols);
    }
    let topRowIds = document.getElementById(`${tableName}Table`).getAttribute('data-theadids');
    topRowIds = JSON.parse(topRowIds);
    formNames.forEach((formName) => {
        let colName = 'col' + formName;
        let col = document.getElementById(colName);
        let colType = col.dataset['type'];
        let input = colType === 'select' ? document.createElement('select') : document.createElement('input');
        if (colType !== 'select') {
            input.type = "text";
            input.step = "any";
        }
        let inputId = (colType === 'select') ? formName.toLowerCase() + 'Select' + loopIndex : formName + loopIndex;
        input.className = "form-control form-control-sm";
        input.id = inputId;
        input.name = inputId;
        input.dataset.lpignore = 'true';
        input.setAttribute('onchange', `syncSingleTableWithForm(${loopIndex}, '${formName}', '${tableName}')`);
        input.setAttribute('oninput', `syncSingleTableWithForm(${loopIndex}, '${formName}', '${tableName}')`);
        if (topRowIds) {
            topRowIds.forEach(topRowId => {
                input.dataset[topRowId] = "";
            });
        }
        if (colType === 'select') {
            input.innerHTML = document.getElementById('colSelect' + formName).innerHTML;
        }

        let displayColNames = col.dataset['displayname'];
        let formGroupDiv = document.createElement('div');
        formGroupDiv.className = "col mb-3";
        formGroupDiv.id = `${formName}FormGroupCol`;
        if (formName === 'id') {
            formGroupDiv.style.display = 'none';
        }
        let label = document.createElement('label');
        label.className = "control-label";
        label.htmlFor = inputId;
        label.textContent = displayColNames;

        formGroupDiv.appendChild(label);
        formGroupDiv.appendChild(input);

        formElements.push(formGroupDiv);
    });
    return formElements;
}

function getTableHeadElems(tableName) {
    const table = document.getElementById(`${tableName}Table`);
    let ids = JSON.parse(table.dataset['headelems']);
    let elements = [];
    ids.forEach(id => {
        const elem = document.getElementById(id);
        if (elem) elements.push(elem);
    });
    return elements
}

function findInQuerySelectorAll(findName, selectorIdVal, selectorPrefix = '', tableName = '') {
    let table = document.getElementById(tableName);
    let selector = `${selectorPrefix}[id^='${selectorIdVal}']`;
    let selectorElems = table.querySelectorAll(selector);
    let selectorElem = Array.from(selectorElems).find(elem => {
        return elem.innerHTML.trim() === findName});
    if (selectorElem) {
        return selectorElem.id.replace(selectorIdVal, '');
    }
}

function getRowFormNames(tableName) {
    let tableHeadElems = getTableHeadElems(tableName);
    return tableHeadElems.filter(col => col.dataset['form'] === 'true').map(col => col.id.substring(3));
}

function checkCellPickCol(loopIndex) {
    let pickVal = document.getElementById(`rowcell_pick_col${loopIndex}`);
    if (pickVal) {
        let curRow = document.getElementById(`tr${loopIndex}`);
        let pickValue = Number(pickVal.innerHTML).toFixed(1);
        Array.from(curRow.children).forEach(cell => {
            let cellValue = Number(cell.innerHTML).toFixed(1);
            if (cellValue === pickValue) {
                cell.click();
            }
        });
    }
}

function createVisibleRow(loopIndex, curTable, tableName, rowData) {
    let tr = document.createElement('tr');
    tr.id = `tr${loopIndex}`;
    tr.setAttribute('data-bs-toggle', curTable.getAttribute('data-accordion') || '');
    tr.setAttribute('data-bs-target', `#collapseRow${loopIndex}`);
    tr.className = 'accordion-toggle';
    tr.setAttribute('data-table', tableName);
    let rowOnClick = curTable.getAttribute('data-rowclick');
    if (rowOnClick) {
        tr.setAttribute('onclick', `getTableOnClick(event, '${rowOnClick}')`);
    }
    tr.innerHTML = getRowHtml(loopIndex, tableName, rowData);
    return tr
}

function createHiddenRow(loopIndex, tableName, bodyId) {
    let hiddenTr = document.createElement('tr');
    hiddenTr.id = `trHidden${loopIndex}`;
    let td = document.createElement('td');
    td.colSpan = getTableHeadElems(tableName).length;
    td.className = 'hiddenRow';
    let divCollapse = document.createElement('div');
    divCollapse.id = `collapseRow${loopIndex}`;
    divCollapse.className = 'collapse';
    divCollapse.setAttribute('aria-labelledby', `heading${loopIndex}`);
    divCollapse.setAttribute('data-bs-parent', `#${bodyId}`);
    let cardBody = document.createElement('div');
    cardBody.className = 'card-body';
    let formHolderDiv = document.createElement('div');
    formHolderDiv.className = `${tableName}FormHolder`;
    formHolderDiv.id = `${tableName}FormHolder${loopIndex}`;
    let formElements = buildFormFromCols(loopIndex, getRowFormNames(tableName), tableName);
    let form = document.createElement('form');
    form.id = `form${tableName}${loopIndex}`;
    form.className = 'row';
    formElements.forEach(element => {
        form.appendChild(element);
    });
    form.autocomplete = "off";
    form = addDeleteBtn(loopIndex, tableName, form);
    formHolderDiv.appendChild(form);
    cardBody.appendChild(formHolderDiv);
    divCollapse.appendChild(cardBody);
    td.appendChild(divCollapse);
    hiddenTr.appendChild(td);
    return hiddenTr;
}

function addRowToTable(rowData, tableName, customTableCols) {
    let curTable = document.getElementById(tableName + 'Table');
    let bodyId = tableName + 'Body';
    let tbody = document.getElementById(bodyId);
    if (!tbody) {
        tbody = document.createElement('tbody');
        tbody.id = bodyId;
        curTable.appendChild(tbody);
    }
    let loopIndex = parseInt(curTable.getAttribute('data-totalrows')) + 1;
    let tr = createVisibleRow(loopIndex, curTable, tableName, rowData);
    let hiddenTr = createHiddenRow(loopIndex, tableName, bodyId);
    let fragment = document.createDocumentFragment();
    fragment.appendChild(tr);
    fragment.appendChild(hiddenTr);
    tbody.appendChild(fragment);
    addDatePicker(`#datePicker${loopIndex}`);
    addSelectize(`#trHidden${loopIndex} [id$='Select${loopIndex}']`);
    addOnClickEvent(`#${tableName}TableTHead [id^=topRowHeader]`, editTopRowOnClick);
    sortTable(bodyId, tableName + 'TableHeader');
    if (customTableCols) {
        for (let customFunc of customTableCols) {
            applyCustomFunction(customFunc, loopIndex);
        }
    }
    checkCellPickCol(loopIndex);
    curTable.setAttribute('data-totalrows', loopIndex.toString());
    return loopIndex
}

function checkSetDate(rowData, loopIndex, rowFormNames) {
    if ('start_date' in rowData) {
        let datePickerElement = document.getElementById("datePicker" + loopIndex);
        if (datePickerElement) {
            datePickerElement.setAttribute('data-start-date', rowData.start_date);
            datePickerElement.setAttribute('data-end-date', rowData.end_date);
            rowFormNames = removeValues(rowFormNames, ['start_date', 'end_date']);
        }
    }
    return rowFormNames
}

function addRowDetailsToForm(rowData, loopIndex, tableName, customTableCols) {
    let topRowElem = '';
    let topRowsName = document.getElementById(tableName + 'TableSlideCol').getAttribute('data-value');
    if (topRowsName in rowData) {
        let topRowCurName = rowData[topRowsName];
        let topRowIndex = findInQuerySelectorAll(topRowCurName,
            'row' + topRowsName, '', tableName);
        topRowElem = document.getElementById('topRowHeader' + tableName + topRowIndex);
        topRowElem.click();
    }
    let rowFormNames = getRowFormNames(tableName);
    rowFormNames =  checkSetDate(rowData, loopIndex, rowFormNames);
    rowFormNames.forEach((rowFormName) => {
        let colName = 'col' + rowFormName;
        let colElem = document.getElementById(colName);
        let inputCheck = colElem.dataset['type'] === 'select';
        let currentElemId = (inputCheck) ? rowFormName.toLowerCase() + 'Select' + loopIndex : rowFormName + loopIndex;
        let curElem = document.getElementById(currentElemId);
        let name = colElem.getAttribute('data-name');
        let newValue = getCellContent(rowData, name);
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
        if (colElem.dataset['type'] === 'slider_edit_col') {
            curElem.style.display = 'none';
            buildSliderEditCol(curElem.parentElement, newValue, curElem.id, loopIndex);
        }
    });
    syncTableWithForm(loopIndex, rowFormNames, tableName);
    let cellClass = `shadeCell${loopIndex % 10}`;
    shadeDates(loopIndex, null, cellClass, tableName);
    if (topRowElem) {
        topRowElem.click();
    }
    if (customTableCols) {
        for (let customFunc of customTableCols) {
            applyCustomFunction(customFunc, loopIndex);
        }
    }
}

function deleteRow(loopIndex, tableName) {
    let preIds = (loopIndex < 0) ? [`topRowHeader${tableName}`] : ['tr', 'trHidden'];
    preIds.forEach(preId => {
        let elem = document.getElementById(`${preId}${loopIndex}`);
        elem.click();
        elem.remove();
    });
    populateTotalCards(tableName);
}

function applyCustomFunction(customFunc, loopIndex) {
    let func = customFunc['func'];
    let args = customFunc['args'].slice();
    args.push(loopIndex);
    window[func].apply(null, args);
}

function addRow(rowData = null, tableName, customTableCols) {
    let loopIndex = addRowToTable(rowData, tableName, customTableCols);
    if (rowData) {
        addRowDetailsToForm(rowData, loopIndex, tableName, customTableCols);
    }
    toggleMetrics(tableName, loopIndex);
    return loopIndex;
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
        let metrics = (metricElem === null) ? {cpc: 0, cpm: 0} : metricElem.dataset;
        return Object.assign({}, {name: e, total_budget: 0,
            start_date: "{{ object.start_date }}", end_date: "{{ object.end_date }}"},
            metrics)
    });
    newRowsData.forEach(newRowData => {
        addRow(newRowData, tableName, );
    });
    addElemRemoveLoadingBtn(this.id);
}

function addDeleteBtn(loopIndex, tableName, form) {
    let deleteButtonDiv = document.createElement('div');
    deleteButtonDiv.className = 'col mb-3';
    let deleteButton = document.createElement('button');
    deleteButton.id = `deleteRow${loopIndex}`;
    deleteButton.className = 'btn w-100 btn-outline-danger text-start';
    deleteButton.type = 'button';
    deleteButton.innerHTML = '<i class="bi bi-trash" role="button" aria-hidden="true"></i> DELETE';
    deleteButton.setAttribute('onclick', `deleteRow(${loopIndex}, '${tableName}');`);
    deleteButtonDiv.appendChild(deleteButton);
    form.insertBefore(deleteButtonDiv, form.firstChild);
    return form
}

function addTopRowCard(tableName, loopIndex, topRowData = null) {
    let topRowName = document.getElementById(tableName + 'TableSlideCol').getAttribute('data-value');
    let formNames = [topRowName, 'total_budget', 'id'];
    let topRowCardId = 'topRowCard' + tableName + loopIndex;
    let topRowFormId = 'topRowForm' + tableName + loopIndex;
    let topRowCard = document.createElement('div');
    topRowCard.id = topRowCardId;
    topRowCard.className = "card shadow outer text-center";
    topRowCard.style.display = 'none';
    let cardHeader = document.createElement('div');
    cardHeader.className = "card-header";
    let cardBody = document.createElement('div');
    cardBody.className = "card-body";
    let topRowForm = document.createElement('form');
    topRowForm.id = topRowFormId;
    topRowForm.className = "";
    let formElements = buildFormFromCols(loopIndex, formNames, tableName);
    formElements.forEach(element => {
        topRowForm.appendChild(element);
    });
    topRowForm = addDeleteBtn(loopIndex, tableName, topRowForm);
    cardBody.appendChild(topRowForm);
    topRowCard.appendChild(cardHeader);
    topRowCard.appendChild(cardBody);
    let elem = document.getElementById(tableName + 'TableSlide');
    elem.appendChild(topRowCard);
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
        let dateElemId = "datePicker" + loopIndex;
        let dateElem = document.getElementById(dateElemId);
        dateElem.setAttribute('data-start-date', topRowData.start_date);
        dateElem.setAttribute('data-end-date', topRowData.end_date);
        addDatePicker('#' + dateElemId);
        let idElem = document.getElementById(`id${loopIndex}`);
        idElem.value = topRowData.id;
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
        setTopRowMetrics(currentIndex, tableName);
    }
}

function toggleMetrics(tableName, loopIndex = null) {
    let selElem = document.getElementById(`selectColumns${tableName}`);
    if (!(selElem)) return;
    let selectize = selElem.selectize;
    let availOptions = Object.keys(selectize.options);
    let selected = selectize.getValue();
    let tableElemId = (loopIndex) ? `tr${loopIndex}` : tableName;
    let tableElem = document.getElementById(tableElemId);
    availOptions.forEach(o => {
        let elems = tableElem.querySelectorAll("[id*='" + o + "']");
        let display = (selected.includes(o)) ? "" : "none";
        elems.forEach(e => {
            e.style.display = display;
        })
    })
}

function toggleMetricsOnChange(e) {
    let tableName = e.target.id.replace('selectColumns', '');
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

function addDatePickerOnEvent() {
    if (!this._flatpickr) {
        flatpickr(this, {
            mode: "range",
            onClose: function(selectedDates, dateStr, instance) {
                let loopIndex = instance.input.id.replace('datePicker', '');
                let shadeColor = loopIndex.replace('-', '');
                let tableName = instance.input.getAttribute('data-table');
                shadeDates(loopIndex, null, 'shadeCell' + shadeColor, tableName);
            },
            onReady: function(selectedDates, dateStr, instance) {
                let startDate = instance.input.getAttribute('data-start-date');
                let endDate = instance.input.getAttribute('data-end-date');
                if (startDate && endDate) {
                    instance.setDate([startDate, endDate], true);
                }
            }
        });
    }
}

function addDatePicker(selector = '[id^=datePicker]') {
    addOnClickEvent(selector, addDatePickerOnEvent, 'focus');
}

function addRows(rows, tableName, customTableCols) {
    rows.forEach(row => {
        addRow(row, tableName, customTableCols);
    });
}

function parseApplyRowHighlight(loopIndex, curColElem, currentValue, curElem) {
    let curRow = document.getElementById('tr' + loopIndex);
    let highlightRow = {
        'comp_val' : currentValue,
        'full_row': true,
        'comparator': 'default',
        'true_color': 'shadeCell0',
        'false_color': '',
    }
    if (curColElem) {
        highlightRow = JSON.parse(decodeURIComponent(curColElem.dataset['highlight_row']));
    }
    if ((curRow) && (highlightRow)) {
        let comparisonOperatorsHash = {
            'default': function(a, b) { return true; },
            '<': function(a, b) { return a < b; },
            '>': function(a, b) { return a > b; },
            '>=': function(a, b) { return a >= b; },
            '<=': function(a, b) { return a <= b; },
            '===': function(a, b) { return a === b; },
            'includes': function(a, b) {return b.includes(a)}
        };
        let comparator = comparisonOperatorsHash[highlightRow['comparator']];
        let fullRow = highlightRow['full_row'];
        let elemToColor = (fullRow) ? curRow : curElem;
        let compValues = highlightRow['comp_val'];
        let trueColor = highlightRow['true_color'];
        let falseColor = highlightRow['false_color'];
        let colorsToAddRemove = (comparator(currentValue, compValues)) ? [trueColor, falseColor] : [falseColor, trueColor];
        (colorsToAddRemove[0]) ? elemToColor.classList.add(colorsToAddRemove[0]) : '';
        (colorsToAddRemove[1]) ? elemToColor.classList.remove(colorsToAddRemove[1]) : '';
    }
}

function syncSingleTableWithForm(loopIndex, formName, tableName, topRowToggle = false) {
    let colName = 'col' + formName;
    let curColElem = document.getElementById(colName);
    let inputCheck = curColElem.dataset['type'] === 'select';
    let currentElemId = (inputCheck) ? formName.toLowerCase() + 'Select' + loopIndex : formName + loopIndex;
    let currentElem = document.getElementById(currentElemId);
    let currentValue = (inputCheck) ? currentElem.selectize.getValue() : currentElem.value;
    if (!inputCheck && !topRowToggle) {
        let topRowHeader = document.getElementById(`${tableName}TableTHead`);
        let topRowElemIdSelector = 'topRowHeader' + tableName;
        let selectedElem = topRowHeader.querySelectorAll(`[id^='${topRowElemIdSelector}'].shadeCell`);
        if (selectedElem.length !== 0) {
            let topRowId = selectedElem[0].id.replace(topRowElemIdSelector, '');
            currentElem.dataset[topRowId] = currentElem.value;
        }
        else {
            let topRowIds = document.getElementById(`${tableName}Table`).getAttribute('data-theadids');
            topRowIds = JSON.parse(topRowIds);
            if (topRowIds) {
                let topRowVal = currentElem.value;
                if (!curColElem.dataset['type'].includes('metrics')) {
                    topRowVal = topRowVal / topRowIds.length;
                }
                topRowIds.forEach(topRowId => {
                    currentElem.dataset[topRowId] = topRowVal;
                })
            }
        }
    }
    if (formName.toLowerCase().includes('cost')) {
        currentValue = formatNumber(Number(currentValue));
        currentValue = '$' + currentValue;
    }
    let curElem = document.getElementById('row' + formName + loopIndex);
    parseApplyRowHighlight(loopIndex, curColElem, currentValue, curElem);
    let isButtonCol = curColElem.dataset['type'] === 'button_col';
    if (isButtonCol) {
        let btnElem = document.getElementById(`btn${formName}${loopIndex}`);
        currentValue += btnElem.outerHTML;
    }
    curElem.innerHTML = currentValue;
    if (loopIndex < 0) {
        let addPlaceId = 'addRowsPlaceholder' + tableName;
        let addPlaceElem = document.getElementById(addPlaceId);
        let colName = addPlaceElem.children[0].id.replace('selectAdd', '').replace('Placeholder' + tableName, '');
        let otherElem = document.getElementById(`row${colName}${loopIndex}`);
        otherElem.innerHTML = currentValue;
    }
    populateTotalCards(tableName);
    if (curColElem.dataset['type'].includes('metrics')) {
        let formNames = getMetricsForTotalCards(tableName);
        formNames.forEach(fn => {
            if (fn[0] === formName) {
                updateMetricCell(fn, loopIndex, null);
            }
        });
    }
}

function syncTableWithForm(loopIndex, formNames, tableName) {
    formNames.forEach((formName) => {
        syncSingleTableWithForm(loopIndex, formName, tableName);
    });
}

function formatNumber(currentNumber, fractionDigits = 0) {
    return currentNumber.toFixed(fractionDigits).toLocaleString("en-US").replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function createTotalCards(tableName, defaultTotalVal = 0) {
    let elem = document.getElementById(tableName);
    elem.insertAdjacentHTML('afterbegin',`
        <div class="card shadow outer text-center">
            <div class="card-header">
                <h5>Metric Totals</h5>
                <h6 class="card-subtitle mb-2 text-muted">Total metric values
                    for the current plan.</h6>
            </div>
            <div class="card-body">
                <div class="container-fluid text-center">
                    <div id="${tableName}TotalCards" data-default="${defaultTotalVal}"
                        class="row w-100 card-group"></div>
                </div>
            </div>
        </div>`
    )
}

function getMetricsForTotalCards(tableName) {
    let formNames = [
        ['', 'total_budget', 1],
        ['cpm', 'Impressions', 0],
        ['cpc', 'Clicks', 0],
        ['cplpv', 'Landing Page', 0],
        ['cpbc', 'Button Clicks', 0],
        ['cpv', 'Views', 0],
        ['cpcv', 'Video Views 100', 0]]
    let cols = getTableHeadElems(tableName);
    cols.forEach(col => {
        if (col.dataset['type'] === 'metrics') {
            let colName = col.dataset['name'];
            let existsInFormNames = formNames.some(([prefix, name]) => name === colName);
            if (!existsInFormNames) {
                formNames.push(['', colName, 1]);
            }
        }
    });
    formNames = formNames.filter(([prefix, name]) => {
        return cols.some(col => col.dataset['name'] === name || col.dataset['name'] === prefix);
    });
    return formNames
}

function getRowCost(rowNum) {
    let totalElem = document.getElementById(`rowtotal_budget${rowNum}`);
    return (totalElem) ? parseFloat(totalElem.innerHTML.replace('$', '')) : 0;
}

function updateMetricCell(formName, rowNum, rowCost) {
    let costPerName = formName[0];
    let summableName = formName[1];
    let targetColName = formName[formName[2]];
    let elemId = `row${targetColName}${rowNum}`;
    let elem = document.getElementById(elemId);
    let rowValue = parseFloat(elem.innerHTML.replace('$', ''));
    if (costPerName !== '') {
        if (!(rowCost)) {
            rowCost = getRowCost(rowNum);
        }
        rowValue = rowCost / rowValue;
        if (summableName === 'Impressions') {
            rowValue = rowValue * 1000;
        }
        let rowId = `row${summableName}${rowNum}`;
        let row = document.getElementById(rowId);
        if (row) {
            row.innerHTML = formatNumber(rowValue);
        }
    }
    return rowValue
}

function populateTotalCards(tableName) {
    let totalCardsElem = document.getElementById(`${tableName}TotalCards`);
    if (!totalCardsElem) return;
    let table = document.getElementById(tableName + 'Body');
    let formatter = new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
    });
    let formNames = getMetricsForTotalCards(tableName);
    let data = [];
    let defaultSumVal = totalCardsElem.dataset['default'];
    defaultSumVal = (defaultSumVal) ? parseFloat(defaultSumVal) : 0;
    data = formNames.map(function (e) {
        return {name: e[1], numeric_value: defaultSumVal, current_value: defaultSumVal, msg: '', change: ''}
    });
    if (table) {
        let cols = getTableHeadElems(tableName);
        let rowName = cols[0].id.replace('col', '');
        let currentRows = table.querySelectorAll(`[id^='row${rowName}']`);
        currentRows.forEach(currentRow => {
            let rowNum = currentRow.id.replace(`row${rowName}`, '');
            if (!isNaN(rowNum[0])) {
                let rowCost = getRowCost(rowNum);
                formNames.forEach(formName => {
                    let rowValue = updateMetricCell(formName, rowNum, rowCost);
                    let summableName = formName[1];
                    let idx = data.findIndex(x => x.name === summableName);
                    rowValue = isNaN(rowValue) ? 0 : rowValue;
                    data[idx]['numeric_value'] += rowValue;
                });
            }
        });
    }
    let idxSumCost = data.findIndex(x => x.name === 'total_budget');
    let sumCost = (data[idxSumCost]) ? (data[idxSumCost]['numeric_value']) : 0;
    formNames.forEach(formName => {
        let costPerName = formName[0];
        let summableName = formName[1];
        let idx = data.findIndex(x => x.name === summableName);
        data[idx]['current_value'] = formatNumber(data[idx]['numeric_value'], 2);
        if (costPerName !== '') {
            let curVal = sumCost / data[idx]['numeric_value'];
            if (summableName === 'Impressions') {
                curVal = curVal * 1000;
            }
            data.push({
                name: costPerName,
                numeric_value: curVal,
                current_value: formatter.format(curVal)
            })
        } else {
            data[idx]['msg'] = "Of Total Budget";
            data[idx]['change'] = sumCost / parseFloat("{{ object.total_budget }}");
        }
    });
    generateTotalCards(tableName + 'TotalCards', data);
    toggleMetrics(tableName);
}

function getTopRowIds(tableName) {
    let table = document.getElementById(tableName);
    let topRowIdSelector = 'topRowHeader' + tableName;
    let topRowElems = table.querySelectorAll(`[id^="${topRowIdSelector}"]`);
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
    let table = document.getElementById(`${tableName}Table`);
    let rowName = table.getAttribute('data-value');
    let tHeadIds = getTopRowIds(tableName);
    table.setAttribute('data-theadids', JSON.stringify(tHeadIds));
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
    shadeDates(minId, [topRowData.start_date, topRowData.end_date], colorClass, tableName);
    addTopRowCard(tableName, minId, topRowData);
    addDataToFormForNewTopRow(tableName, minId);
    tHeadIds = getTopRowIds(tableName);
    table.setAttribute('data-theadids', JSON.stringify(tHeadIds));
}

function addCurrentTopRows(topRowsData, tableName) {
    topRowsData.forEach(topRowData => {
        addTopRow(topRowData, tableName);
    });
    turnOffAllCards(tableName);
}

function generateDisplayColumnName(colName) {
    return colName
        .toUpperCase()
        .replace('ESTIMATED_', 'e')
        .replace(/_/g, ' ');
}

function addTableColumn(col, specifyFormCol, thead, name) {
    let colName = col['name'];
    let formCol = (!(specifyFormCol)) ? 'true' : col['form'];
    let highlightRow = encodeURIComponent(JSON.stringify(existsInJson(col, 'highlight_row')));
    let displayName = generateDisplayColumnName(colName);
    thead.innerHTML += `
            <th data-form="${formCol}" data-type="${col['type']}"
                data-highlight_row="${highlightRow}" data-name="${colName}"
                data-tableid="${name}Table" data-link="${col['link_col']}"
                data-displayname="${displayName}"
                id="col${colName}">${displayName}
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
            Object.entries(val).forEach(([k, v]) => {
                optionData += `data-` + k + `="` + v + `" `;
            });
            colSelectElem.innerHTML += `
                    <option ` + optionData + `
                            value="` + val[colName] + `">` + val[colName] + `</option>`;
        });
        if (col['add_select_box']) {
            let elem = document.getElementById('addRowsPlaceholder' + name);
            let placeHolderName = 'selectAdd' + colName + 'Placeholder' + name;
            let newElem = `
                    <div id="${placeHolderName}" class="p-0">Select ${colName}...</div>
                    <div class="input-group-append">
                        <button id="addRows${name}"
                                class="btn btn-outline-success w-100 text-start"
                                type="button" href="">
                            <i class="bi bi-plus-lg"  role="button"></i>
                        </button>
                    </div>`;
            elem.insertAdjacentHTML('beforeend', newElem);
            let addBoxName = colName.toLowerCase() + 'SelectAdd';
            document.getElementById(placeHolderName).innerHTML = `
                <select id="${addBoxName}" multiple="" class="width100">
                    <option value="">Select ${colName}s To Add...</option>
                </select>`;
            document.getElementById(addBoxName).innerHTML += document.getElementById(selectName).innerHTML;
        }
        addSelectize();
    }
    if (col['type'].includes('metrics')) {
        let colSelectElem = document.getElementById(`selectColumns${name}`);
        if (colSelectElem) {
            colSelectElem = colSelectElem.selectize;
            colSelectElem.addOption({
                text: displayName,
                id: colName,
                value: colName
            });
            if (col['type'] === 'default_metrics') {
                let curVal = colSelectElem.getValue();
                curVal.push(colName);
                colSelectElem.setValue(curVal);
            }
        }
    }
    return thead
}

function addTableColumns(cols, name) {
    let tHeadName =  name + 'TableTHead';
    let table = document.getElementById(name + 'Table');
    table.innerHTML += `<thead id="${tHeadName}"><tr id="${name}TableHeader"></tr></thead>`;
    let thead = document.getElementById(name + 'TableHeader');
    let selectColsElem = document.getElementById('selectColumnsToggle' + name);
    if (selectColsElem) {
        selectColsElem.innerHTML = `
            <select id="selectColumns${name}" multiple="" class="width100 form-control">
                <option value="">Select Columns To Add...</option>
            </select>`;
        addSelectize();
        addOnClickEvent('[id^="selectColumns"]', toggleMetricsOnChange, 'change');
    }
    let specifyFormCol = table.getAttribute('data-specifyform');
    let addHiddenCol = false;
    let colIds = [];
    cols.forEach(col => {
        thead = addTableColumn(col, specifyFormCol, thead, name);
        addHiddenCol = (col['type'] === 'cell_pick_col') ? true : addHiddenCol;
        colIds.push(`col${col['name']}`);
    });
    table.setAttribute('data-headelems', JSON.stringify(colIds));
    if (addHiddenCol) {
        let pickCol = document.getElementById('colcell_pick_col');
        if (pickCol) {
            pickCol.style.display = 'none';
            pickCol.dataset['type'] = 'metrics';
        } else {
            let col = {
                'type': 'metrics',
                'hidden': true,
                'name': 'cell_pick_col'
            }
            addTableColumn(col, specifyFormCol, thead, name);
        }
    }
}

function convertColsToObject(cols) {
    return cols.map(x => {
        return ({name: x, type: '', add_select_box: false,
            hidden: false, header: false, form: false});
    });
}

function getColumnValues(columnIndex, rows) {
    // Get an array of values for the given column index
    let values = [];
    rows.forEach(row => {
        let cell = row.getElementsByTagName("td")[columnIndex];
        let value = cell.textContent || cell.innerText;
        if (!values.includes(value)) {
            values.push(value);
        }
    })
    return values;
}

function closeFilterDialog() {
    let tableId = this.dataset.tableId;
    let filterDialogs = document.querySelectorAll(`[id^='colFilterBox${tableId}']`);
    filterDialogs.forEach(elem => {
        elem.style.display = 'none';
    });
}

function showFilterDialog() {
    closeFilterDialog.call(this);
    let tableId = this.dataset.tableId;
    let colIdx = this.dataset.colIdx;
    let dialogDisplay = document.getElementById(`colFilterBox${tableId}${colIdx}`);
    dialogDisplay.style.display = '';
    dialogDisplay.style.top = 'auto';
    dialogDisplay.style.left = 'auto';
    dialogDisplay.style.zIndex = 'auto';
}

function filterTable() {
    let tableId = this.dataset.tableid;
    let table = document.getElementById(tableId);
    let colIdx = this.dataset.colidx;
    let searchValueId = `colFilterBoxSearch${tableId}${colIdx}`;
    const searchValue = document.getElementById(searchValueId).value;
    let col = table.rows[0].cells[colIdx];
    let checkboxes = col.querySelectorAll('input[type="checkbox"]');
    const selectedValues = new Set();
    let isSelectAll = (this.dataset.curvalue === 'Select All');
    checkboxes.forEach(elem => {
        if (isSelectAll) {
            elem.checked = (this.checked)
        }
        if ((elem.dataset.curvalue !== 'Select All') && (elem.checked)) {
            selectedValues.add(elem.dataset.curvalue);
        }
    });
    let rows = table.querySelectorAll("tr:not([id*='Hidden']):not([id*='Header'])");
    rows.forEach(row => {
        const cell = row.cells[colIdx];
        const showRow =
            cell.textContent.toLowerCase().includes(searchValue.toLowerCase()) &&
            selectedValues.has(cell.textContent);
        row.style.display = showRow ? "" : "none";
    })
}

function createTableFilter(tableId) {
    const table = document.getElementById(tableId);

    // Get the table rows
    const rows = table.querySelectorAll("tr:not([id*='Hidden']):not([id*='Header'])");

    const valuesByColumn = new Map();

    // Extract unique values in each column
    for (let j = 0; j < table.rows[0].cells.length; j++) {
        const values = getColumnValues(j, rows)
        valuesByColumn.set(j, values);
    }

    function createDialog(j, values) {
        const headerCell = table.rows[0].cells[j];
        const dialog = document.createElement("div");
        dialog.classList.add("card", "shadow", "popover");
        dialog.style.display = "none";
        dialog.id = `colFilterBox${tableId}${j}`;
        dialog.dataset.tableId = tableId;
        dialog.dataset.colIdx = j;
        headerCell.appendChild(dialog);
        const dialogHeader = document.createElement("div");
        dialogHeader.classList.add("card-header");
        dialogHeader.dataset.tableId = tableId;
        dialogHeader.onclick = closeFilterDialog;
        dialog.appendChild(dialogHeader);
        const closeIcon = document.createElement("i");
        closeIcon.classList.add("bi", "bi-filter");
        closeIcon.onclick = closeFilterDialog;
        closeIcon.dataset.tableId = tableId;
        dialogHeader.appendChild(closeIcon);
        const input = document.createElement("input");
        input.id = `colFilterBoxSearch${tableId}${j}`;
        input.type = "text";
        input.placeholder = "Search...";
        input.classList.add("form-control");
        input.dataset.tableid = tableId;
        input.dataset.colidx = j;
        dialog.appendChild(input);
        addOnClickEvent('#' + input.id, filterTable, 'input');
        dialog.appendChild(document.createElement("br"));
        values.unshift("Select All");
        values.forEach(function (value, i) {
            let filterPrefix = 'colFilterSwitchItem';
            let switchId = `${filterPrefix}${tableId}${j}${i}`;
            let elemToAdd = `
                <div class="form-check form-switch">
                    <input data-tableid="${tableId}" data-colidx="${j}" data-curvalue="${value}"
                         type="checkbox" checked class="form-check-input" id="${switchId}">
                    <label class="form-check-label" for="${switchId}">${value}</label>
                </div>
            `
            dialog.insertAdjacentHTML('beforeend', elemToAdd);
            addOnClickEvent('#' + switchId, filterTable, 'change', false);
        });

        // Add the filter icon
        const icon = document.createElement("i");
        icon.classList.add("bi", "bi-filter");
        icon.id = `colFilterIcon${tableId}${j}`
        icon.dataset.tableId = tableId;
        icon.dataset.colIdx = j;
        headerCell.appendChild(icon);
        addOnClickEvent('#' + icon.id, showFilterDialog, 'click');
    }

    for (let j = 0; j < table.rows[0].cells.length; j++) {
        const values = valuesByColumn.get(j);
        createDialog(j, values);
    }
}

function addProgressBars(progCol, color, loopIndex) {
    let cell = document.getElementById(`row${progCol}${loopIndex}`);
    let value = (cell.textContent) ? cell.textContent : '0';
    cell.classList.add('table-progress');
    cell.classList.add('p-0');
    cell.textContent = '';
    value = value.replace('%', '');
    value = value.replace(/^\s+|\s+$|\n/g, '');
    let progBar = `<div class="progress table-progress-cell m-0">
                        <div class="progress-bar bg-${color}" role="progressbar"
                           style="width: ${value}%;" aria-valuenow="${value}" 
                           aria-valuemin="0" aria-valuemax="100">${value}</div>
                   </div>`;
    cell.innerHTML += progBar;
}

function addTrendingArrows(col, loopIndex) {
    let cell = document.getElementById(`row${col}${loopIndex}`);
    let value = (cell.textContent) ? cell.textContent : '0';
    value = parseFloat(value);
    let icon;
    if (value > 0) {
        icon = `<i>  </i><i class="bi bi-arrow-up text-success"></i>`;
    }
    if (value < 0) {
        icon = `<i>  </i><i class="bi bi-arrow-down text-danger"></i>`;
    }
    cell.innerHTML += icon
}

function showChart(tableName, chartShow) {
    let elem = document.getElementById(`${tableName}ChartCol`);
    let tableElem = document.getElementById(`${tableName}TableBaseCol`);
    let showChartBtn = document.getElementById(`showChartBtn${tableName}`);
    if (elem.style.display === 'none' || chartShow) {
        elem.style.display = '';
        tableElem.style.display = 'none';
        showChartBtn.classList.remove('btn-outline-success');
        showChartBtn.classList.add('btn-success');
    }
    else {
        elem.style.display = 'none';
        tableElem.style.display = '';
        showChartBtn.classList.remove('btn-success');
        showChartBtn.classList.add('btn-outline-success');
    }
}

function createLiquidTableChart(tableName, tableRows, chartFunc,
                                chartMetadata=null) {
    let chartElemId = `${tableName}ChartPlaceholder`;
    let chartElem = document.getElementById(chartElemId);
    chartElem.style.display = '';
    let headElems = getTableHeadElems(tableName);
    let xCols = [];
    let yCols = [];
    headElems.forEach(elem => {
        let firstCellId = elem.id.replace('col', 'row') + '0';
        let cell = document.getElementById(firstCellId);
        if (cell) {
            let value = cell.textContent;
            value = value.replace(/[$%,]/g, '');
            let cellName = elem.id.replace('col', '');
            (isNaN(value)) ? xCols.push(cellName) : yCols.push(cellName);
        }
    });
    showChart(tableName, true);
    if (xCols.length > 1 && chartFunc !== "generateTreeMap") {
        tableRows = combineColumns(tableRows, xCols);
        xCols = [xCols.join('_')];
    }
    if (yCols.length) {
        if (chartFunc) {
            let generateChart = window[chartFunc];
            document.getElementById(chartElemId).innerHTML = '';
            if (chartMetadata) {
                generateChart(`#${chartElem.id}`, tableRows,
                    ...Object.values(chartMetadata));
            }
            else {
                generateChart(`#${chartElem.id}`, tableRows, xCols[0], yCols);
            }
            addSelectOptions(tableRows, xCols[0], false, "", false)
        }
        else {
            generateBarChart(`#${chartElem.id}`, tableRows, xCols[0], yCols);
        }
    }
}

function getTableOnClick(e, imgToGet) {
    let elem = e.target;
    if (elem.tagName.toLowerCase() === 'a') {
        return
    }
    elem = elem.parentElement;
    let tableId = elem.dataset['table'];
    let table = document.getElementById(tableId);
    let imgElemId = imgToGet;
    let rowIndex = elem.id.replace('tr', '');
    let uniqueCols = getTableHeadElems(tableId);
    let vk = '';
    uniqueCols.forEach(col => {
        let colName = col.dataset['name'];
        vk += document.getElementById(`row${colName}${rowIndex}`).textContent;
        vk += '|';
    });
    let elemToAdd = `<div id="${imgElemId}"></div>`;
    table.insertAdjacentHTML('beforebegin', elemToAdd);
    let imgElem = document.getElementById(imgElemId);
    imgElem.innerHTML = '';
    parseApplyRowHighlight(rowIndex, null, '', '');
    getTable(imgToGet, imgElem.id, 'None', vk);
}

async function downloadLiquidTable(tableId, asArray = false, name = null,
                                   binary = false) {
    tableId = typeof (tableId) === 'string' ? tableId : this.dataset['table_name'];
    let liquid_table_args = getMetadata(tableId);
    let tableElem = document.getElementById(tableId + 'TableBaseCol');
    if (!tableElem) {return [{}]}
    if (tableElem.style.display === 'none') {
        let chartElem = document.getElementById(tableId + 'ChartPlaceholder');
        let svg = chartElem.querySelector('svg');
        let svgStyles = document.getElementById('customSvgStyles');
        let jinjaValues = document.getElementById('jinjaValues').dataset;
        name = (name) ? name : jinjaValues['title'] + "_" + jinjaValues['object_name'] + "_" + tableId + ".png";
        let data;
        if (binary) {
            data = await downloadSvg(svg, svgStyles, name, binary);
        }
        else {
            data = downloadSvg(svg, svgStyles, name, binary);
        }
        return [data]
    } else if (asArray) {
        return getTableAsArray(tableId + 'TableBaseCol');
    } else {
        getTable('downloadTable', this.id, 'None', tableId,
            'None', true, 'None', false, liquid_table_args);
    }
}

function filterLiquidTable() {
    let tableName = this.id.replace('FilterButton', '');
    let filterElemId = `${tableName}Filter`;
    let filterElem = document.getElementById(filterElemId);
    let selectElems = filterElem.querySelectorAll(`select`);
    let filterDict = getDates(`#Date${filterElemId}`);
    selectElems.forEach(selectElem => {
        filterDict = getFilters(selectElem.id, filterDict);
    });
    filterDict = {'filter_dict': filterDict}
    getTable(tableName, tableName, 'None', 'None', 'None',
        true, 'None', false, filterDict)
}

function buildFilterDict(tableName, filterDict) {
    const table = document.getElementById(tableName);
    if (!table) return;
    const filterRow = document.createElement('div');
    filterRow.classList.add('row');
    filterRow.id = `${tableName}Filter`;
    for (let key in filterDict) {
        const cell = document.createElement('div');
        cell.classList.add('col');
        let name= `${key}${tableName}Filter`;
        let select = '';
        if (key === 'Date') {
            select = document.createElement('input');
            select.classList.add('form-control');
            select.classList.add('form-control-sm');
            select.classList.add('flatpickr');
            select.classList.add('flatpickr-input');
            select.classList.add('active');
            select.id = name;
        } else {
            select = document.createElement('select');
            select.name = key;
            select.id = name + 'Select';
            select.multiple = true;
            const option = document.createElement('option');
            option.value = '';
            option.textContent = `Select ${key}...`;
            select.appendChild(option);
            filterDict[key].forEach(value => {
                const option = document.createElement('option');
                option.value = value;
                option.textContent = value;
                select.appendChild(option);
            });
        }
        cell.appendChild(select);
        filterRow.appendChild(cell);
    }
    const button = document.createElement('div');
    button.classList.add('btn');
    button.classList.add('btn-outline-primary');
    button.classList.add('col');
    button.textContent = 'Apply Filter';
    let buttonId = `${tableName}FilterButton`;
    button.id = buttonId;
    filterRow.appendChild(button);
    table.insertBefore(filterRow, table.firstChild);
    addOnClickEvent('#' + buttonId, filterLiquidTable);
    let datePicker = document.getElementById(`Date${tableName}Filter`);
    if (datePicker) {
        addDatePicker('#' + datePicker.id)
    }
}

function addMetadata(tableName, metadata) {
    const table = document.getElementById(tableName);
    for (const [key, value] of Object.entries(metadata)) {
        table.dataset[key] = JSON.stringify(value);
    }
}

function getMetadata(elementId) {
    const element = document.getElementById(elementId);
    const data = {};
    for (const key in element.dataset) {
        if (key !== 'called') {
            data[key] = JSON.parse(element.dataset[key]);
        }
    }
    return data;
}

function editInlineOnBlur() {
    let formElem = this;
    if (!(formElem)) {return}
    let formElemId = (formElem.id.includes('datePicker')) ? formElem.id.replace('datePicker', 'start_date') : formElem.id;
    let tmpFormElemId = `${formElem.id}TMP`;
    let tmpFormElem = document.getElementById(tmpFormElemId);
    tmpFormElem.insertAdjacentElement('afterend', formElem);
    let rowElem = document.getElementById(`row${formElemId}`);
    rowElem.style.display = '';
}

function editInlineOnClick() {
    let rowElem = this;
    if (!(rowElem)) {return}
    let formElemId = rowElem.id.replace('row', '');
    formElemId = (formElemId.includes('start_date')) ? formElemId.replace('start_date', 'datePicker') : formElemId;
    let formElem = document.getElementById(formElemId);
    let tmpFormElemId = `${formElem.id}TMP`;
    let tmpFormElem = document.getElementById(tmpFormElemId);
    if (!(tmpFormElem)) {
        formElem.insertAdjacentHTML('afterend', `<div id="${tmpFormElemId}"></div>`);
    }
    rowElem.style.display = 'none';
    rowElem.insertAdjacentElement('afterend', formElem);
    addOnClickEvent(`#${formElem.id}`, editInlineOnBlur,'blur');
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
    let customTableCols = existsInJson(tableData, 'custom_cols');
    let tableAccordion = existsInJson(tableData, 'accordion');
    let specifyFormCols = existsInJson(tableData, 'specify_form_cols');
    let colDict = existsInJson(tableData, 'col_dict');
    let rowOnClick = existsInJson(tableData, 'row_on_click');
    let newModalBtn = existsInJson(tableData, 'new_modal_button');
    let colFilter = existsInJson(tableData, 'col_filter');
    let searchBar = existsInJson(tableData, 'search_bar');
    let chartBtn = existsInJson(tableData, 'chart_btn');
    let chartFunc = existsInJson(tableData, 'chart_func');
    let chartShow = existsInJson(tableData, 'chart_show');
    let tableButtons = existsInJson(tableData, 'table_buttons');
    let filterDict = existsInJson(tableData, 'filter_dict');
    let metadata =  existsInJson(tableData, 'metadata');
    let inlineEdit =  existsInJson(tableData, 'inline_edit');
    let chartArgs =  existsInJson(tableData, 'chart_args');
    if (!(colDict)) {
        tableCols = convertColsToObject(tableCols);
    }
    createTableElements(tableName, rowsName, topRowsName, title,
        description, colToggle, tableAccordion, specifyFormCols, rowOnClick,
        newModalBtn, colFilter, searchBar, chartBtn, tableButtons);
    addTableColumns(tableCols, tableName);
    if (topRowsName) {
        addCurrentTopRows(tableTopRows, tableName);
    }
    if (tableRows) {
        addRows(tableRows, tableName, customTableCols);
    }
    if (totalCards) {
        let defaultTotalVal = existsInJson(tableData, 'total_default_val');
        createTotalCards(tableName, defaultTotalVal);
        populateTotalCards(tableName);
    }
    if (colFilter) {
        createTableFilter(tableName + 'Table');
    }
    if (chartBtn) {
        createLiquidTableChart(tableName, tableRows, chartFunc, chartArgs);
        showChart(tableName, chartShow);
    }
    if (filterDict) {
        buildFilterDict(tableName, filterDict);
    }
    if (metadata) {
        addMetadata(tableName, metadata);
    }
    addSelectize();
    addOnClickEvent(`#${tableName} button[id^=addRows]`, addRowsOnClick);
    addOnClickEvent(`#${tableName} button[id^=addTopRow]`, addTopRowOnClick);
    addOnClickEvent(`#${tableName} button[id^=downloadBtn]`, downloadLiquidTable);
    if (inlineEdit) {
        addOnClickEvent('td[id^=row]', editInlineOnClick);
    }
}
