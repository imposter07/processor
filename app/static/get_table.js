function turnOffProgress(downloadingProgress, oldHtml, clickElem) {
    if (oldHtml !== 'None') {
        $(clickElem).html(oldHtml);
    } else {
        addElemRemoveLoadingBtn(clickElem);
    }
    unanimateBar();
    clearInterval(downloadingProgress);
    downloadingProgress = null;
    let downloadID = (clickElem.indexOf('request_table-') !== -1) ? 'downloadProgress' + clickElem : 'downloadProgress';
    let downloadElem = document.getElementById(downloadID);
    if (downloadElem) {
        downloadElem.style.width = '100%';
    }
    unanimateBar(downloadID);
}

function downloadTableResponse(tableName, pond, vendorKey, data) {
    data.blob().then((data) => {
        let mimeType = 'text/csv';
        let extension = '.csv';
        if (tableName === 'OutputDataSOW') {
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
            document.getElementById('downloadProgressBaseClass').remove();
        } else {
            pond.addFile(blob);
            let jinjaValues = document.getElementById('jinjaValues').dataset;
            let link = document.createElement('a');
            link.href = window.URL.createObjectURL(blob);
            link.download = jinjaValues['title']  + "_" + jinjaValues['object_name'] + "_" + tableName + "_" + vendorKey + extension;
            link.click();
            let elem = document.getElementById('downloadBarPond');
            elem.parentElement.remove();
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
        } else if (['screenshot', 'notesTable'].includes(tableName)) {
            createLiquidTable(data, {'tableName': data['data']['name']});
        }
        else {
            show_modal_table('modalTableButton');
            let newTableName = data['data']['name'];
            let tableCols = data['data']['cols'];
            let tableData = data['data']['data'];
            if (newTableName === 'modal-body-table') {
                let modalTable = document.getElementById("modal-body-table");
                modalTable.innerHTML = "";
                createLiquidTable(data, {'tableName': newTableName});
            }
            else {
                createTable(tableCols, tableData, newTableName);
            }
        }
    })
}

function getTableComplete(tableName, pond, vendorKey, data){
    if ((tableName === 'OutputDataRawDataOutput') ||
        (tableName === 'download_raw_data') ||
        (tableName === 'download_pacing_data') ||
        (tableName === 'OutputDataSOW') ||
        (tableName === 'OutputDataTopline') ||
        (tableName === 'screenshotImage' )
    ) {
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
                let downloadID = (clickElem.indexOf('request_table-') !== -1) ? '#downloadProgress' + clickElem : '#downloadProgress';
                let downloadProgress = $(downloadID);
                let newPercent = data['percent'];
                if (downloadProgress) {
                    let oldPercent = parseInt(downloadProgress.attr("style").match(/\d+/)[0]);
                    if (newPercent > oldPercent) {
                        if (updateFunction) {
                            updateFunction(newPercent);
                        } else {
                            downloadProgress.attr("style", "width: " + newPercent + "%")
                        }
                    } else {
                        let percent = oldPercent + 2;
                        if (updateFunction) {
                            updateFunction(percent);
                        } else {
                            downloadProgress.attr("style", "width: " + percent + "%")
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
    let downloadProgress = document.getElementById('downloadProgress');
    downloadProgress.style.width = '100%';
    // window.location.reload(true);
    if (forceReturn) {
        turnOffProgress(downloadingProgress, oldHtml, clickElem);
    }
}

async function getTable(tableName, clickElem, oldHtml, vendorKey='None',
                  pond='None', progress=true, fixId='None',
                        forceReturn=false) {
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