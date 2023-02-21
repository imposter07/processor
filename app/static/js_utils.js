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

function sortTable(bodyName, tableHeaderId) {
    const getCellValue = (tr, idx) => tr.children[idx].innerText || tr.children[idx].textContent;
    const comparer = (idx, asc) => (a, b) => ((v1, v2) =>
            v1 !== '' && v2 !== '' && !isNaN(v1.replace(/[$%]/g,'')) && !isNaN(v2.replace(/[$%]/g,'')) ? v1.replace(/[$%]/g,'') - v2.replace(/[$%]/g,'') : v1.toString().localeCompare(v2)
    )(getCellValue(asc ? a : b, idx), getCellValue(asc ? b : a, idx));
    // document.getElementById(tableHeaderId).innerHTML = document.getElementById(tableHeaderId).innerHTML;
    function sortTableEvent() {
        const table = document.getElementById(bodyName);
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
    addOnClickEvent("th:not([id*='thHidden'])", sortTableEvent, 'click', false);
}

function addOnClickEvent(elemSelector, clickFunction, type = 'click', preventDefault = true) {
    for (let elm of document.querySelectorAll(elemSelector)) {
        elm.removeEventListener(type, clickFunction);
        if (preventDefault) {
            elm.addEventListener(type, function (e) {
                e.preventDefault();
            });
        }
        elm.addEventListener(type, clickFunction);
    }
}

function loadingBtn(elem, currentStyle = '', btnClass="btn btn-primary btn-block") {
    elem.style.display = 'none';
    elem.insertAdjacentHTML('beforebegin', `
        <button id="loadingBtn" class="${btnClass}"
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
        document.getElementById('loadingBtn').remove();
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
    let oldDownload = document.getElementById('downloadProgressBaseClass');
    if (oldDownload) {
        oldDownload.remove();
    }
    $(
        '<div id="downloadProgressBaseClass" class="progress">\n' +
        '  <div id="downloadProgress" ' +
               'class="progress-bar bg-success progress-bar-striped progress-bar-animated" ' +
               'role="progressbar" ' +
        'style="width: 1%" aria-valuenow="1" aria-valuemin="0" aria-valuemax="100"></div>\n' +
        '</div>').insertAfter(currentElement);
    animateBar();
    if (pond) {
        $('<div class="form-group" style="height:96px">' +
         '<input id="downloadBarPond" type="file"></div>)').insertAfter(currentElement);
        const inputElement = document.querySelector('input[id="downloadBarPond"]');
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