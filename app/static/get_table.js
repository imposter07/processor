function turnOffProgress(oldHtml, clickElem) {
    if (oldHtml !== 'None') {
        $(clickElem).html(oldHtml);
    } else {
        addElemRemoveLoadingBtn(clickElem);
    }
    unanimateBar();
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
        } else if (tableName === 'OutputDataToplineDownload') {
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

function parseTableResponse(tableName, pond, vendorKey, data, callbackFunc) {
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
        } else if (tableName === 'getLog') {
            parseLog(data);
        } else if (tableName === 'apply_processor_plan') {
            appendCardAsTable(data, "newPlanResultsCardCol", "#rowOne", 'NEW PLAN RESULTS', false);
        } else if (tableName === 'get_plan_property') {
            document.getElementById('rowOne').innerHTML = '';
            let title = 'PLAN PROPERTY - ' + vendorKey;
            appendCardAsTable(data, "planPropertyCardCol", "#rowTwo", title, true);
        } else if (tableName === 'change_dictionary_order') {
            showModalTable('modalTableButton');
            let newTableName = data['data']['name'];
            createChangeDictOrder(data['data']['cols'], data['data']['data'],
                newTableName, data['dict_cols'], data['relational_cols']);
        } else if (existsInJson(data['data'], 'liquid_table')) {
            let newTableName = data['data']['name'] ? data['data']['name'] : tableName;
            const modalName = 'modal-body-table';
            let newTable = document.getElementById(newTableName);
            newTable.innerHTML = "";
            if (newTableName === modalName) {
                showModalTable('modalTableButton');
            }
            createLiquidTable(data, {'tableName': newTableName});
        } else if (tableName === 'Pacing Table') {
            generatePacingTable(tableName, data['data']['data'], data['plan_cols'])
        } else if (callbackFunc) {
            callbackFunc(data, true, tableName)
        }
        else {
            showModalTable('modalTableButton');
            let newTableName = data['data']['name'];
            let tableCols = data['data']['cols'];
            let tableData = data['data']['data'];
            createTable(tableCols, tableData, newTableName);
        }
    })
}

function getTableComplete(tableName, pond, vendorKey, data, callbackFunc){
    let dlTables = [
        'OutputDataRawDataOutput', 'download_raw_data', 'download_pacing_data',
        'OutputDataSOW', 'OutputDataToplineDownload', 'screenshotImage', 'billingInvoice',
        'downloadTable'];

    let outputTableName = 'OutputData' + tableName;
    tableName = (dlTables.includes(outputTableName)) ? outputTableName : tableName;
    if (dlTables.includes(tableName) || (dlTables.includes(tableName))) {
        downloadTableResponse(tableName, pond, vendorKey, data);
    }
    else {
        parseTableResponse(tableName, pond, vendorKey, data, callbackFunc);
    }
}

function getCompletedTask(tableName, procId = null, task = null,
                          pond = 'None', vendorKey = 'None',
                          fixId = 'None', args='None',
                          callbackFunc=null) {
    let jinjaValues = document.getElementById('jinjaValues').dataset;
    let uploaderType = (jinjaValues['title'] === "Uploader") ? jinjaValues['uploader_type'] : "None";
    let data = {
        object_type: jinjaValues['title'],
        object_name: jinjaValues['object_name'],
        object_level: jinjaValues['edit_name'],
        uploader_type: uploaderType,
        task_name: tableName,
        object_id: procId,
        task: task,
        table: tableName,
        fix_id: fixId,
        vendorkey: vendorKey,
        args: args
    }
    let formData = convertDictToFormData(data);
    fetch('/get_completed_task', {
        method: 'POST',
        body: formData
    }).then((data) => {
        getTableComplete(tableName, pond, vendorKey, data, callbackFunc);
    });
}

function getTaskProgressResponse(data, kwargs) {
    let forceReturn = kwargs['forceReturn'];
    let tableName = kwargs['tableName'];
    let procId = kwargs['object_id'];
    let task = kwargs['task'];
    let pond = kwargs['pond'];
    let vendorKey = kwargs['vendorKey'];
    let oldHtml = kwargs['oldHtml'];
    let clickElem = kwargs['clickElem'];
    let fixId = kwargs['fixId'];
    let args = kwargs['args'];
    let updateFunction = kwargs['updateFunction'];
    let callbackFunc = kwargs['callbackFunc'];
    if ('complete' in data && data['complete']) {
        turnOffProgress(oldHtml, clickElem);
        if (!forceReturn) {
            getCompletedTask(tableName, procId, task, pond,
                vendorKey, fixId, args, callbackFunc);
        }
    } else {
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
        setTimeout(getTaskProgress, 2500, tableName, updateFunction,
            procId, task,forceReturn, pond, vendorKey, oldHtml, clickElem,
            fixId, args, callbackFunc)
    }
}

function getTaskProgress(tableName, updateFunction = false,
                         procId = 'None', task = null, forceReturn = false,
                         pond = 'None', vendorKey = 'None', oldHtml = null,
                         clickElem = null, fixId = null, args='None',
                         callbackFunc=null) {
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
        vendorkey: vendorKey,
        args: args
    };
    let kwargs = {
        'object_type': jinjaValues['title'],
        'object_name': jinjaValues['object_name'],
        'object_level': jinjaValues['edit_name'],
        'forceReturn': forceReturn,
        'tableName': tableName,
        'object_id': procId,
        'task': task,
        'pond': pond,
        'vendorKey': vendorKey,
        'oldHtml': oldHtml,
        'clickElem': clickElem,
        'fixId': fixId,
        'args': args,
        'updateFunction': updateFunction,
        'callbackFunc': callbackFunc
    };
    makeRequest('/get_task_progress', 'POST', data,
        getTaskProgressResponse, 'json', kwargs, getTableError);
}

function getTableResponse(data, kwargs) {
    let forceReturn = kwargs['forceReturn'];
    let tableName = kwargs['tableName'];
    let pond = kwargs['pond'];
    let vendorKey = kwargs['vendorKey'];
    let oldHtml = kwargs['oldHtml'];
    let clickElem = kwargs['clickElem'];
    let fixId = kwargs['fixId'];
    let args = kwargs['args'];
    let callbackFunc = kwargs['callbackFunc'];
    let procId = (args !== 'None' && 'proc_id' in args) ? args['proc_id'] : 'None';
    if (forceReturn) {
        getTableComplete(tableName, pond, vendorKey, data, callbackFunc);
    } else {
        if (data['task']) {
            getTaskProgress(tableName, false, procId, data['task'],
                forceReturn, pond, vendorKey, oldHtml, clickElem, fixId, args,
                callbackFunc);
        }
    }
    if (forceReturn) {
        turnOffProgress(oldHtml, clickElem);
    }
}

function getTableError(error, kwargs) {
    let forceReturn = kwargs['forceReturn'];
    let oldHtml = kwargs['oldHtml'];
    let clickElem = kwargs['clickElem'];
    let dlId = 'downloadProgress' + clickElem;
    let downloadProgress = document.getElementById(dlId);
    downloadProgress.style.width = '100%';
    // window.location.reload(true);
    if (forceReturn) {
        turnOffProgress(oldHtml, clickElem);
    }
}

function setDownloadBarAndLoadingBtn(elemId) {
    let elem = document.getElementById(elemId);
    if (elem) {
        setDownloadBar(elem, '', false);
        loadingBtn(elem);
    }
}

async function getTable(tableName, clickElem, oldHtml = 'None', vendorKey= 'None',
                  pond='None', progress= true, fixId= 'None',
                  forceReturn= false, args='None', callbackFunc) {
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
        args: args,
        force_return: forceReturn
    };
    let procId = (args !== 'None' && 'proc_id' in args) ? args['proc_id'] : 'None';
    if (progress && forceReturn) {
        getTaskProgress(tableName, false, procId,
            null, forceReturn, pond, vendorKey, oldHtml, clickElem, fixId,
            callbackFunc);
    }
    let kwargs = {
        'forceReturn': forceReturn,
        'tableName': tableName,
        'pond': pond,
        'vendorKey': vendorKey,
        'oldHtml': oldHtml,
        'clickElem': clickElem,
        'fixId': fixId,
        'args': args,
        'callbackFunc': callbackFunc,
    };
    makeRequest('/get_table', 'POST', data, getTableResponse, 'json',
        kwargs, getTableError);
}

function sendDataTableResponse(data, kwargs) {
    let jinjaValues = document.getElementById('jinjaValues').dataset;
    let tableName = kwargs['tableName'];
    let newPage = kwargs['newPage'];
    let formContinue = kwargs['formContinue'];
    let oldPage = kwargs['oldPage'];
    let saveBtnElemId = kwargs['saveBtnElemId'];
    if (['Import', 'Fees'].includes(jinjaValues['edit_name'])) {
        reloadPage();
    } else {
        $('#' + tableName).modal('hide');
        displayAlert(data['message'], data['level']);
    }
    if (newPage) {
        completeSave(data, formContinue, oldPage, newPage);
        unanimateBar();
    }
    addElemRemoveLoadingBtn(saveBtnElemId);
}

function SendDataTable(tableName="modalTable", formContinue = null,
                       oldPage = '', newPage = '', data = '', cols = [],
                       saveButton = '') {
    let sendTableName = tableName;
    if (!data) {
        let sourceElem = document.querySelectorAll(`table[id^=modalTable]`);
        if (sourceElem.length === 0) {
            if (newPage) {
                completeSave({'message': 'Saved!', 'level': 'success'},
                    formContinue, oldPage, newPage);
            }
            else {
                return false
            }
        }
        else {
            sourceElem = sourceElem[0].id;
        }
        if (sourceElem.includes('change_dictionary_order')) {
            removeChangeOrderSelectize();
            data = getTableAsArray(sourceElem, cols);
            sendTableName = sourceElem.replace('modalTable', '');
        } else if (sourceElem.includes('apply_processor_plan')) {
            completeSave({'message': 'Saved!', 'level': 'success'},
                formContinue, oldPage, newPage);
        }
        else {
            let sendTable = '';
            try {
                sendTable = $('#' + sourceElem).DataTable();
            } catch (err) {
                sendTable = $('#' + sourceElem).DataTable();
            }
            sendTableName = sourceElem.replace('modalTable', '');
            data = sendTable
                .data().toArray();
        }
    }
    let saveExists = (saveButton === '');
    let saveBtnId = (saveExists) ? tableName + 'SaveButton' : saveButton;
    let saveBtnElem = document.getElementById(saveBtnId);
    if (saveBtnElem) {
        loadingBtn(saveBtnElem);
    } else {
        saveBtnElem = document.querySelectorAll('[id^=loadingBtn]')[0];
        saveBtnElem = document.getElementById(saveBtnElem.id.replace('loadingBtn', ''));
    }
    let jv = document.getElementById('jinjaValues');
    let title = jv.dataset['title'];
    let uploaderType = (title === "Uploader") ? jv.dataset['uploader_type'] : "None";
    let requestData = {
        data: JSON.stringify(data),
        object_name: jv.dataset['object_name'],
        object_type: title,
        object_level: jv.dataset['edit_name'],
        uploader_type: uploaderType,
        table: sendTableName
    }
    let kwargs = {
        'tableName': tableName,
        'newPage': newPage,
        'formContinue': formContinue,
        'oldPage': oldPage,
        'saveBtnElemId': saveBtnElem.id
    }
    makeRequest('/post_table', 'POST', requestData, sendDataTableResponse, 'json', kwargs);
}
