function convertDictToFormData(data) {
    if (data instanceof FormData) {
        return data
    }
    const formData = new FormData();
    for (const name in data) {
        if (typeof data[name] === 'object' && data[name] !== 'None') {
            formData.append(name, JSON.stringify(data[name]));
        }
        else {
            formData.append(name, data[name]);
        }
    }
    return formData
}

function convertFormDataToDict(formData) {
    if (!(formData instanceof FormData)) {
        return formData
    }
    let data = {};
    for (const key of formData.keys()) {
        data[key] = formData.getAll(key);
        if (data[key].length === 1) {
            data[key] = data[key][0];
        }
    }
    return data
}

function prepareFormsForRequest(formIds) {
    let data = {};
    formIds.forEach(formId => {
        let form = document.getElementById(formId);
        let formData = new FormData(form);
        data[form.id] = JSON.stringify(
            convertFormDataToDict(formData));
    })
    return data
}

function validateFormsById(formIds) {
    let valid = true;
    formIds.forEach(formId => {
        if (!document.getElementById(formId).reportValidity()) {
            valid = false;
        }
    })
    return valid
}

function makeRequest(url, method, data, responseFunction,
                     responseType = 'json', kwargs = {},
                     errorFunction = '') {
    let formData = convertDictToFormData(data);
    fetch(url, {
        method: method,
        body: formData
    }).then((data) => {
        if (responseType === 'json') {
            data.json().then((data) => {
                responseFunction(data, kwargs);
            });
        } else {
            responseFunction(data, kwargs);
        }
    }).catch(error => {
        console.log("Request failed: " + error);
        if (errorFunction) {
            errorFunction(error, kwargs);
        }
    });
}

function searchTable(tableName, selector = ' tr:not(.header)') {
    const searchInputId = `#tableSearchInput${tableName.replace('#', '')}`;
    const trs = document.querySelectorAll(tableName + selector);
    const filter = document.querySelector(searchInputId).value;
    const regex = new RegExp(filter, 'i');
    const isThInChildren = child => child.tagName === 'TH';
    const isTh = childrenArr => childrenArr.some(isThInChildren);
    const isFoundInTds = td => regex.test(td.innerHTML);
    const isFound = childrenArr => childrenArr.some(isFoundInTds);
    const setTrStyleDisplay = ({style, children}) => {
        style.display = (isFound([
            ...children // <-- All columns
        ]) || isTh([
            ...children // <-- All columns
        ])) ? '' : 'none'
    }
    trs.forEach(setTrStyleDisplay);
}

function searchForms(formID, selector = 'div[class*="card col-"]', query = '') {
    let filter = document.querySelector('#formSearchInput').value.trim()
        .toLowerCase();
    if (query) {
        filter = query.trim().toLowerCase()
    }
    const forms = document.getElementById(formID);
    const searchableElements = forms.querySelectorAll(selector);
    for (const element of searchableElements) {
        let hidden = true;
        const inputs = element.querySelectorAll('input[class*="form-control"]');
        for (const input of inputs) {
            if (input && 'value' in input) {
                const value = input.value.trim().toLowerCase();
                if (value.includes(filter)) {
                    hidden = false;
                    break
                }
            }
        }
        if (hidden) {
            element.style.display = 'none';
        } else {
            element.style.display = '';
        }
    }
}

function sortTableEvent() {
    const fullTable = document.getElementById(this.dataset.tableid);
    const table = fullTable.querySelector('tbody');
    const getCellValue = (tr, idx) => tr.children[idx].innerText || tr.children[idx].textContent;
    const comparer = (idx, asc) => (a, b) => ((v1, v2) =>
            v1 !== '' && v2 !== '' && !isNaN(v1.replace(/[$%]/g, '')) && !isNaN(v2.replace(/[$%]/g, '')) ? v1.replace(/[$%]/g, '') - v2.replace(/[$%]/g, '') : v1.toString().localeCompare(v2)
    )(getCellValue(asc ? a : b, idx), getCellValue(asc ? b : a, idx));
    let sortArrow = this.getElementsByClassName('fas');
    if (sortArrow.length > 0) {
        let downArrow = sortArrow[0].classList.contains('fa-arrow-down');
        let arrowReplace = (downArrow) ? ['down', 'up'] : ['up', 'down'];
        sortArrow[0].className = sortArrow[0].className.replace(arrowReplace[0], arrowReplace[1]);
    } else {
        this.insertAdjacentHTML('beforeend', `<i class="fas fa-arrow-down" href="#" role="button"></i>`);
    }
    Array.from(table.querySelectorAll("tr:not([id*='Hidden']):not([id*='Header'])"))
        .sort(comparer(Array.from(this.parentNode.children).indexOf(this), this.asc = !this.asc))
        .forEach(function (tr) {
            let loopIndex = tr.id.replace('tr', '');
            let hiddenElem = document.getElementById('trHidden' + loopIndex);
            hiddenElem.remove();
            table.appendChild(tr);
            table.appendChild(hiddenElem);
        });
}

function sortTable(bodyName, tableHeaderId) {
    addOnClickEvent("th:not([id*='thHidden'])", sortTableEvent, 'click', false);
}

function addOnClickEvent(elemSelector, clickFunction, type = 'click', preventDefault = true,
                         remove=true) {
    for (let elm of document.querySelectorAll(elemSelector)) {
        if ((elm.tagName === 'SELECT') && (elm.selectize)) {
            if (remove) {
                $('#' + elm.id).unbind()
            }
            $('#' + elm.id).on(type, function (e) {
                clickFunction(e);
            });
        } else {
            if (remove) {
                elm.removeEventListener(type, clickFunction);
            }
            if (preventDefault) {
                elm.addEventListener(type, function (e) {
                    e.preventDefault();
                });
            }
            elm.addEventListener(type, clickFunction);
        }
    }
}

function loadingBtn(elem, currentStyle = '', btnClass="btn btn-primary btn-block") {
    let loadingBtnId = 'loadingBtn' + elem.id;
    elem.style.display = 'none';
    elem.insertAdjacentHTML('beforebegin', `
        <button id="${loadingBtnId}" class="${btnClass}"
            style="${currentStyle}" type="button" disabled>
          <span class="spinner-grow spinner-grow-sm" role="status" aria-hidden="true"></span>
          Loading...
        </button>`);
}

function existsInJson(jsonData, jsonKey) {
    return (jsonData.hasOwnProperty(jsonKey) || jsonKey in jsonData) ? jsonData[jsonKey] : ''
}

function addElemRemoveLoadingBtn(elemId) {
    if (elemId) {
        let loadingBtnId = 'loadingBtn' + elemId;
        let loadingElem = document.getElementById(loadingBtnId);
        if (loadingElem) {
            loadingElem.remove();
        }
        let elem = document.getElementById(elemId);
        elem.style.display = '';
    }
}

function animateBar(){
    let d = document.getElementById("progressBar");
    if (d) {
        d.className += " progress-bar-animated";
    }
}

function unanimateBar(barId = "progressBar"){
    let d = document.getElementById(barId);
    if (d) {
        d.className = d.className.replace( /(?:^|\s)progress-bar-animated(?!\S)/g , '' );
    }
}

function removeValues(arr1, arr2) {
  return arr1.filter(function(value) {
    return arr2.indexOf(value) === -1;
  });
}

function checkIfExists(arr1, arr2) {
  return arr2.some(function(value) {
    return arr1.indexOf(value) !== -1;
  });
}

function setDownloadBar(currentElement, textContent, pond = true) {
    let dlProgStr = 'downloadProgress';
    let dlProgBaseId = dlProgStr + 'BaseClass' + currentElement.id;
    let dlProgId = dlProgStr + currentElement.id;
    let oldDownload = document.getElementById(dlProgBaseId);
    if (oldDownload) {
        oldDownload.remove();
    }
    let elemToAdd = `
        <div id="${dlProgBaseId}" class="progress">
            <div id="${dlProgId}"
                 class="progress-bar bg-success progress-bar-striped progress-bar-animated"
                 role="progressbar"
                 style="width: 1%" aria-valuenow="1" aria-valuemin="0" aria-valuemax="100">
            </div>
        </div>`
    currentElement.insertAdjacentHTML('afterend', elemToAdd);
    animateBar();
    if (pond) {
        let dlPondId = 'downloadBarPond' + currentElement.id;
        let pondElemToAdd = `
            <div class="form-group" style="height:96px">
                <input id="${dlPondId}" type="file">
            </div>
        `
        currentElement.insertAdjacentHTML('afterend', pondElemToAdd);
        const inputElement = document.querySelector(`input[id="${dlPondId}"]`);
        let pondElem = FilePond.create(inputElement,
            {files: [
                {
                    source: '12345',
                    options: {
                        type: 'local',
                        file: {
                            name: textContent + '.csv',
                            type: 'text/csv'
                        }
                    }
                }
            ],
            allowPaste: false,
            credits: false
            });
        pondElem.instantUpload = false;
        return pondElem
    }
}

function viewSelectorChangeEvent(e) {
    window.location = e.target.selectize.getValue();
}

function toggleNav () {
    let elem = document.getElementById("navbarToggler");
    let main = document.getElementsByTagName("main")[0];
    main.style.marginLeft = (elem.classList.contains('show')) ?
         "25%" : "0%";
    main.classList = (elem.classList.contains('show')) ?
         "col-9" : "col";
}

function hexToRgb(hex) {
    const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
    return result ? `rgba(
    ${parseInt(result[1], 16).toString()},
    ${parseInt(result[2], 16).toString()},
    ${parseInt(result[3], 16.).toString()}, 1)`
        : null;
}

function setAttributes(elem, attributes) {
    Object.entries(attributes).forEach(([key, value]) => {
        elem.setAttribute(key, value);
    })
}

function showModalTable(destElemId) {
    let destElem = document.getElementById(destElemId);
    if (!(destElem)) {
        let jinjaValues = document.getElementById('jinjaValues').dataset;
        let modalElemId = destElemId + 'modalTable';
        let modalHtml = `
            <button id="${destElemId}" type="button" class="btn btn-primary"
            data-toggle="modal" data-target="#${modalElemId}" style="display: none;">
            </button>
            <div class="modal fade bd-example-modal-xl" id="${modalElemId}" tabindex="-1"
                 role="dialog" aria-labelledby="exampleModalLabel">
              <div class="modal-dialog modal-xl mw-100 w-90" role="document">
                <div class="modal-content">
                  <div class="modal-header">
                    <h5 class="modal-title" id="exampleModalLabel">
                        Data Tables - ${jinjaValues['object_name']}</h5>
                    <button type="button" class="close" data-dismiss="modal"
                            aria-label="Close">
                      <span aria-hidden="true">&times;</span>
                    </button>
                  </div>
                  <div class="modal-body">
                      <div id="modal-body-table"></div>
                  </div>
                  <div class="modal-footer">
                    <div class="btn-group btn-group-lg btn-block" role="group" aria-label="Basic example">
                        <button type="button" class="btn btn-success"
                                id="modalTableSaveButton"
                                onclick="SendDataTable()">Save</button>
                        <button type="button" class="btn btn-secondary"
                                data-dismiss="modal">Close</button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
        `
        document.getElementsByTagName('body')[0].insertAdjacentHTML('beforeend', modalHtml);
        destElem = document.getElementById(destElemId);
    }
    destElem.click();
}

function fadeInElement(element) {
    element.style.opacity = '0';
    element.style.display = 'block';
    element.style.transition = 'opacity 0.5s';
    setTimeout(function () {
        element.style.opacity = '1';
    }, 10);
}

function displayAlert(message, level) {
    let alertPlaceholderId = 'alertPlaceholder';
    let alertPlaceholderElem = document.getElementById(alertPlaceholderId);
    if (!(alertPlaceholderElem)) {
        let alertContainer = document.getElementById('alertContainer');
        alertContainer.insertAdjacentHTML('beforeend', '<div id="alertPlaceholder"></div>');
        alertPlaceholderElem = document.getElementById(alertPlaceholderId);
    }
    alertPlaceholderElem.classList.add('alert');
    alertPlaceholderElem.classList.add('alert-' + level);
    alertPlaceholderElem.innerHTML = message;
    let btnHtml = `
        <button type="button" class="close" data-dismiss="alert"
            aria-label="Close">
            <span aria-hidden="true">&times;</span>
        </button>`;
    alertPlaceholderElem.insertAdjacentHTML('beforeend', btnHtml);
    fadeInElement(alertPlaceholderElem);
}

function downloadSvg(svgElem, styleElem = null, name='svg.png') {
    let svgString = (new XMLSerializer()).serializeToString(svgElem);
    if (styleElem) {
        let styleString = (new XMLSerializer()).serializeToString(styleElem);
        svgString = ''.concat(
            svgString.slice(0, -6), styleString, svgString.slice(-6)
        );
    }
    const svgBlob = new Blob([svgString], {
        type: 'image/svg+xml;charset=utf-8'
    });
    const url = URL.createObjectURL(svgBlob);
    const image = new Image();
    image.width = svgElem.width.baseVal.value;
    image.height = svgElem.height.baseVal.value;
    image.onload = function () {
        const canvas = document.createElement('canvas');
        canvas.width = image.width;
        canvas.height = image.height;

        const ctx = canvas.getContext('2d');
        ctx.drawImage(image, 0, 0);
        URL.revokeObjectURL(url);
        const imgURI = canvas.toDataURL('image/png');

        const downloadLink = document.createElement('a');
        downloadLink.download = name;
        downloadLink.target = '_blank';
        downloadLink.href = imgURI;
        downloadLink.click();
    };
    image.src = url;
}

function getFilters(elemId, filterDict) {
    let newFilter = {};
    let elem = document.getElementById(elemId);
    if (elem) {
        let elemKey = (elem.name) ? elem.name : elem.id.replace('Select', '');
        newFilter[elemKey] = elem.selectize.getValue();
        filterDict = filterDict.concat(newFilter);
    }
    return filterDict
}

function getMultipleFilters(filterList, filterDict, appendElem='') {
    filterList.forEach(col => {
        let elemId = `${col}${appendElem}`;
        filterDict = getFilters(elemId, filterDict);
    });
    return filterDict
}