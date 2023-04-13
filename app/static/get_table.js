function turnOffProgress(downloadingProgress, oldHtml, clickElem) {
    if (oldHtml !== 'None') {
        $(clickElem).html(oldHtml);
    } else {
        addElemRemoveLoadingBtn(clickElem);
    }
    unanimateBar();
    clearInterval(downloadingProgress);
    downloadingProgress = null;
    let downloadID = 'downloadProgress' + clickElem;
    let downloadElem = document.getElementById(downloadID);
    if (downloadElem) {
        downloadElem.style.width = '100%';
        downloadElem.parentElement.remove();
    }
    unanimateBar(downloadID);
}

function downloadTableResponse(tableName, pond, vendorKey, data) {
    data.blob().then((data) => {
        let mimeType = 'text/csv';
        let extension = '.csv';
        if (['OutputDataSOW', 'billingInvoice'].includes(tableName)) {
            mimeType = 'application/pdf';
            extension = '.pdf';
        } else if (tableName === 'OutputDataTopline') {
            mimeType = 'application/vnd.ms-excel';
            extension = '.xlsx';
        } else if (tableName === 'screenshotImage') {
            mimeType = 'image/bmp';
        }
        let blob = new Blob([data], {type: mimeType});
        if (tableName === 'screenshotImage') {
            let image = new Image();
            image.src = URL.createObjectURL(blob);
            image.classList.add('col');
            let elem = document.getElementById('screenshotImage');
            elem.appendChild(image);
            let dlBars = document.querySelectorAll(`[id^="downloadProgressBaseClass"]`);
            dlBars.forEach(elem => {
                elem.remove();
            });

        } else if (tableName === 'billingInvoice') {
            let elem = document.getElementById('billingInvoice');
            let iframeId = `iframe${elem.id}`;
            elem.innerHTML = `
                <iframe id=${iframeId} src="" type="application/pdf" width="100%" height="100%" 
                style="overflow: auto;">
                </iframe>`
            document.getElementById(iframeId).src = URL.createObjectURL(blob);
        } else {
            let jinjaValues = document.getElementById('jinjaValues').dataset;
            let link = document.createElement('a');
            link.href = window.URL.createObjectURL(blob);
            link.download = jinjaValues['title']  + "_" + jinjaValues['object_name'] + "_" + tableName + "_" + vendorKey + extension;
            link.click();
            let dlBars = document.querySelectorAll(`[id^="downloadBarPond"]`);
            dlBars.forEach(elem => {
                elem.parentElement.remove();
            });
        }
    })
}

function parseTableResponse(tableName, pond, vendorKey, data) {
    data.json().then((data) => {
        if (tableName === 'raw_file_comparison') {
            parseRawComp(data);
        } else if (tableName === 'request_table') {
            let element = document.getElementById(data['html_data']['data']['name']);
            element.innerHTML += data['msg'];
            element.innerHTML += data['html_data']['data']['data'];
            addCollapse(element, data['html_data']['data']['name']);
        } else if (tableName === 'check_processor_plan') {
            appendMessage(data, 'planCheckCardCol', 'rowZero', 'Plan Check');
        } else if (tableName === 'apply_processor_plan') {
            appendCardAsTable(data, "newPlanResultsCardCol", "#rowOne", 'NEW PLAN RESULTS', false);
        } else if (tableName === 'get_plan_property') {
            document.getElementById('rowOne').innerHTML = '';
            let title = 'PLAN PROPERTY - ' + vendorKey;
            appendCardAsTable(data, "planPropertyCardCol", "#rowTwo", title, true);
        } else if (tableName === 'change_dictionary_order') {
            show_modal_table('modalTableButton');
            let newTableName = data['data']['name'];
            createChangeDictOrder(data['data']['cols'], data['data']['data'],
                newTableName, data['dict_cols'], data['relational_cols']);
        } else if (existsInJson(data['data'], 'liquid_table')) {
            let newTableName = data['data']['name'];
            const modalName = 'modal-body-table';
            if (newTableName === modalName) {
                let modalTable = document.getElementById(modalName);
                modalTable.innerHTML = "";
                show_modal_table('modalTableButton');
            }
            createLiquidTable(data, {'tableName': newTableName});
        } else if (tableName === 'Pacing Table') {
            generatePacingTable(tableName, data['data']['data'], data['plan_cols'])
        } else if (tableName === 'Daily Pacing') {
            generateDailyPacing(tableName, data['data']['data'], data['data']['plan_cols'])
        }
        else {
            show_modal_table('modalTableButton');
            let newTableName = data['data']['name'];
            let tableCols = data['data']['cols'];
            let tableData = data['data']['data'];
            createTable(tableCols, tableData, newTableName);
        }
    })
}

function getTableComplete(tableName, pond, vendorKey, data){
    let dlTables = [
        'OutputDataRawDataOutput', 'download_raw_data', 'download_pacing_data',
        'OutputDataSOW', 'OutputDataTopline', 'screenshotImage', 'billingInvoice'];
    if (dlTables.includes(tableName)) {
        downloadTableResponse(tableName, pond, vendorKey, data);
    }
    else {
        parseTableResponse(tableName, pond, vendorKey, data);
    }
}

function getCompletedTask(tableName, procId = null, task = null,
                          pond = 'None', vendorKey = 'None',
                          fixId = 'None') {
    let jinjaValues = document.getElementById('jinjaValues').dataset;
    let data = {
        object_type: jinjaValues['title'],
        object_name: jinjaValues['object_name'],
        object_level: jinjaValues['edit_name'],
        task_name: tableName,
        object_id: procId,
        task: task,
        table: tableName,
        fix_id: fixId,
        vendorkey: vendorKey
    }
    let formData = convertDictToFormData(data);
    fetch('/get_completed_task', {
        method: 'POST',
        body: formData
    }).then((data) => {
        getTableComplete(tableName, pond, vendorKey, data);
    });
}

function getTaskProgress(tableName, updateFunction = false, downloadingProgress,
                         procId = null, task = null, forceReturn = false,
                         pond = 'None', vendorKey = 'None', oldHtml = null,
                         clickElem = null, fixId = null) {
    let jinjaValues = document.getElementById('jinjaValues').dataset;
    downloadingProgress = setInterval(function() {
        $.post('/get_task_progress',
            {
                object_type: jinjaValues['title'],
                object_name: jinjaValues['object_name'],
                object_level: jinjaValues['edit_name'],
                task_name: tableName,
                object_id: procId,
                task: task,
                table: tableName,
                fix_id: fixId,
                vendorkey: vendorKey
            }).done(function (data) {
                if ('complete' in data && data['complete']) {
                    turnOffProgress(downloadingProgress, oldHtml, clickElem);
                    if (!forceReturn) {
                        getCompletedTask(tableName, procId, task, pond,
                            vendorKey, fixId);
                    }
                }
                else {
                    let downloadID = 'downloadProgress' + clickElem;
                    let downloadProgress = document.getElementById(downloadID);
                    let newPercent = data['percent'];
                    if (downloadProgress) {
                        let oldPercent = parseInt(downloadProgress.getAttribute("style").match(/\d+/)[0]);
                        if (newPercent > oldPercent) {
                            if (updateFunction) {
                                updateFunction(newPercent);
                            } else {
                                downloadProgress.setAttribute("style", "width: " + newPercent + "%")
                            }
                        } else {
                            let percent = oldPercent + 2;
                            if (updateFunction) {
                                updateFunction(percent);
                            } else {
                                downloadProgress.setAttribute("style", "width: " + percent + "%")
                            }
                        }
                    }
                }
        });
    }, 2500);
    return downloadingProgress
}

function getTableResponse(data, kwargs) {
    let forceReturn = kwargs['forceReturn'];
    let tableName = kwargs['tableName'];
    let pond = kwargs['pond'];
    let vendorKey = kwargs['vendorKey'];
    let oldHtml = kwargs['oldHtml'];
    let clickElem = kwargs['clickElem'];
    let fixId = kwargs['fixId'];
    let downloadingProgress = kwargs['downloadingProgress'];
    if (forceReturn) {
        getTableComplete(tableName, pond, vendorKey, data);
    } else {
        if (data['task']) {
            downloadingProgress = getTaskProgress(tableName, false, downloadingProgress,
                null, data['task'], forceReturn, pond, vendorKey, oldHtml, clickElem,
                fixId);
        }
    }
    if (forceReturn) {
        turnOffProgress(downloadingProgress, oldHtml, clickElem);
    }
}

function getTableError(error, kwargs) {
    let forceReturn = kwargs['forceReturn'];
    let downloadingProgress = kwargs['downloadingProgress'];
    let oldHtml = kwargs['oldHtml'];
    let clickElem = kwargs['clickElem'];
    let dlId = 'downloadProgress' + clickElem;
    let downloadProgress = document.getElementById(dlId);
    downloadProgress.style.width = '100%';
    // window.location.reload(true);
    if (forceReturn) {
        turnOffProgress(downloadingProgress, oldHtml, clickElem);
    }
}

function setDownloadBarAndLoadingBtn(elemId) {
    let elem = document.getElementById(elemId);
    setDownloadBar(elem, '', false);
    loadingBtn(elem);
}

async function getTable(tableName, clickElem, oldHtml = 'None', vendorKey= 'None',
                  pond='None', progress= true, fixId= 'None',
                  forceReturn= false) {
    setDownloadBarAndLoadingBtn(clickElem);
    let jinjaValues = document.getElementById('jinjaValues').dataset;
    let uploaderType = (jinjaValues['title'] === "Uploader") ? jinjaValues['uploader_type'] : "None";
    let data = {
        table: tableName,
        object_name: jinjaValues['object_name'],
        object_type: jinjaValues['title'],
        object_level: jinjaValues['edit_name'],
        uploader_type: uploaderType,
        vendorkey: vendorKey,
        fix_id: fixId,
        force_return: forceReturn
    }
    let downloadingProgress = null;
    if (progress && forceReturn) {
        downloadingProgress = getTaskProgress(tableName, false,
            downloadingProgress, null, null, forceReturn,
            pond, vendorKey, oldHtml, clickElem, fixId);
    }
    let kwargs = {
        'forceReturn': forceReturn,
        'tableName': tableName,
        'pond': pond,
        'vendorKey': vendorKey,
        'oldHtml': oldHtml,
        'clickElem': clickElem,
        'fixId': fixId,
        'downloadingProgress': downloadingProgress
    }
    makeRequest('/get_table', 'POST', data, getTableResponse, 'json',
        kwargs, getTableError);
}