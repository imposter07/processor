function convertDictToFormData(data) {
    const formData = new FormData();
    for (const name in data) {
        formData.append(name, data[name]);
    }
    return formData
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
        }
    }).catch(error => {
        console.log("Request failed: " + error);
        if (errorFunction) {
            errorFunction(error, kwargs);
        }
    });
}

function searchTable(tableName, selector = ' tr:not(.header)') {
    const trs = document.querySelectorAll(tableName + selector);
    const filter = document.querySelector('#tableSearchInput').value;
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
