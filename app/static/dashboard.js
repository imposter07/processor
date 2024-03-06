function isElementInViewport(el, offset=30) {
    const rect = el.getBoundingClientRect();
    const windowHeight = window.innerHeight || document.documentElement.clientHeight;
    const windowWidth = window.innerWidth || document.documentElement.clientWidth;

    return (
        rect.top >= -offset &&
        rect.left >= -offset &&
        rect.top <= (windowHeight + offset) &&
        rect.right <= (windowWidth + offset)
    );
}

function callFunctionIfInView(elementId, func, args, offset=30) {
    const element = document.getElementById(elementId);
    if (element && isElementInViewport(element, offset) && !element.dataset.called &&
        !element.classList.contains('d-none')) {
        func(...args);
        element.dataset.called = 'true';
    }
}

function getTabFunctions(tab) {
    let tabs = {
        'dash': getAllCharts,
        'partner': getPartnerMetrics,
        'country': getCountryMetrics,
        'targeting': getTargetingMetrics,
        'placement': getPlacementMetrics,
        'creative': getCreativeMetrics
    }
    return tabs[tab]
}

function getTabCharts() {
    let tabs = document.querySelectorAll('[id^="nav-"][id$="-tab"]');
    tabs.forEach(function (tab) {
        if (tab.classList.contains('active') && tab.id !== 'nav-tab') {
            let tabName = tab.id.split('-')[1];
            let tabFunc = getTabFunctions(tabName);
            if (tabFunc) {
                tabFunc();
            }
        }
    });
}

function getAllCharts(filterDict, clickElem = null, oldHtml = null,
                      allCharts = false, report = false) {
    let metrics = ['kpi'];
    filterDict = (filterDict) ? filterDict : [];
    let total_metric_args = getDataTableArgsDict(
        'generateTotalCards', ['mpProduct Name'], metrics, filterDict);
    let daily_metric_args = getDataTableArgsDict(
        'generateDualLineChart', ['eventdate'], metrics, filterDict, true);
    let kpi_notes_args = getDataTableArgsDict(
        'generateKpiNotes', ['mpProduct Name'], metrics, filterDict);
    let delivery_metric_args = getDataTableArgsDict(
        'generateProgressBars', ['vendorname'], ['netcost', 'plannednetcost'],
        filterDict, true);
    callFunctionIfInView('dailyMetrics', getTable,
        ['dailyMetrics', 'dailyMetricsProgress', 'None', 'None', 'None',
            true, 'None', false, daily_metric_args]);
    callFunctionIfInView('deliveryMetrics', getTable,
        ['deliveryMetrics', 'deliveryMetricsProgress', 'None', 'None', 'None',
            true, 'None', false, delivery_metric_args]);
    callFunctionIfInView('customChartsTopline', buildCustomCharts, ['Topline']);
    if (!report) {
        callFunctionIfInView('totalMetrics', getTable,
            ['totalMetrics', 'totalMetricsProgress', 'None', 'None', 'None',
                true, 'None', false, total_metric_args, getMetricsComplete]);
        callFunctionIfInView('dailyMetricsNotes', getTable,
            ['dailyMetricsNotes', 'dailyMetricsNotesProgress', 'None', 'None', 'None',
                true, 'None', false, kpi_notes_args, getMetricsComplete]);
        callFunctionIfInView('toplineMetrics', getTable,
            ['toplineMetrics', 'toplineMetricsProgress', 'None', filterDict]);
    }
    if (!allCharts) {
        let pacing_alert_args = getDataTableArgsDict(
            'generateAlertCount', ['vendorname'], ['netcost', 'plannednetcost'], filterDict);
        let partner_metric_args = getDataTableArgsDict(
            'generateBarChart', ['vendorname'], metrics, filterDict, true);
        callFunctionIfInView('pacingAlertCount', getTable,
            ['pacingAlertCount', 'pacingAlertCountProgress', 'None', 'None',
                'None', false, 'None', false, pacing_alert_args, getMetricsComplete]);
        callFunctionIfInView('partnerMetrics', getTable,
            ['partnerMetrics', 'partnerMetricsProgress', 'None', 'None',
                'None', true, 'None', false, partner_metric_args]);
    }
    if (allCharts) {
        getSecondaryCharts(filterDict, clickElem);
    }
}

async function getCustomCharts(report=false) {
    let jv = document.getElementById('jinjaValues');
    let object_name = jv.dataset['object_name'].trim();
    let response = await fetch(
        `/processor/${object_name}/dashboard/get?report=${report}`);
    return await response.json();
}

function callCustomChart(dash) {
    let dashId = dash['id'];
    let chartName = `dash${dashId}Metrics`;
    let chartType = dash['chart_type'];
    let chartFunction = getChartFunctionName(chartType);
    let defaultView = dash['default_view'] === 'Chart';
    let dash_metric_args = getDataTableArgsDict(
        chartFunction, JSON.parse(dash['dimensions']),
        JSON.parse(dash['metrics']), JSON.parse(dash['chart_filters']),
        true, undefined, undefined, defaultView);
    callFunctionIfInView(chartName, getTable, [chartName,
        chartName + 'Progress', 'None', 'None', 'None', true, 'None', false, dash_metric_args],
        100);
}

async function buildCustomCharts(tab, build=true) {
    let elem = document.getElementById(`customCharts${tab}`);
    let count = 0;
    let dashboards = await getCustomCharts();
    dashboards.forEach(function (dash) {
        if (tab === dash['tab'] || tab === 'All') {
            if (build) {
                let id = Math.floor(count / 2);
                let row_id = `row_${id}_${elem.id}`;
                if (count % 2 === 0) {
                    let row_html = `<div class="row" id="${row_id}"></div>`;
                    elem.insertAdjacentHTML('beforeend', row_html);
                }
                let row = document.getElementById(row_id);
                let html = `<div class="col-md-6"><br>${dash['html']}</div>`;
                row.insertAdjacentHTML('beforeend', html);
            }
            callCustomChart(dash);
            count++
        }
    });
}

function getSecondaryCharts(filterDict, clickElem = null) {
    let addSelect = !(clickElem);
    let metrics = ['kpi'];
    let rowIds = ['thirdRow', 'fourthRow', 'filterRow'];
    rowIds.forEach(elemId => {
        document.getElementById(elemId).removeAttribute('hidden');
    });
    let cols = ['partner', 'country', 'campaign', 'environment', 'kpi'];
    cols.forEach(function (col, i) {
        let chartId = `${col}Metrics`;
        let colName = (col === 'partner') ? 'vendorname' : `${col}name`;
        let chartType = (i > 1) ? 'generateLollipopChart' : 'generateBarChart';
        let secondary_metric_args = getDataTableArgsDict(
            chartType, [colName], metrics, filterDict, true);
        getTable(chartId, `${chartId}Progress`, 'None', 'None',
            'None', true, 'None', false, secondary_metric_args);
    });
}

function getPartnerMetrics(filterDict = null, clickElem = null, oldHtml = null,
                           allCharts = false, report = false) {
    let metrics = ['kpi'];
    filterDict = (filterDict) ? filterDict : [];
    let pacing_alert_args = getDataTableArgsDict(
        'generateAlerts', ['vendorname', 'campaignname'],
        ['netcost', 'adservingcost'], filterDict);
    let partner_bar_args = getDataTableArgsDict(
        'generateDualBarChart', ['vendorname'],
        metrics, filterDict, true, undefined, undefined, true);
    let partner_metric_args = getDataTableArgsDict(
        'generateBarChart', ['campaignname', 'vendorname'],
        metrics, filterDict, true, undefined, undefined, false);
    let tree_metric_args = getDataTableArgsDict(
        'generateTreeMap', ['vendortypename', 'vendorname'],
        ['netcost'], filterDict, true);
    callFunctionIfInView('partnerBarMetrics', getTable,
        ['partnerBarMetrics', 'partnerBarMetricsProgress', 'None', 'None', 'None',
            true, 'None', false, partner_bar_args]);
    callFunctionIfInView('partnerSummaryMetrics', getTable,
        ['partnerSummaryMetrics', 'partnerSummaryMetricsProgress', 'None', 'None', 'None',
            true, 'None', false, partner_metric_args]);
    callFunctionIfInView('partnerTreeMetrics', getTable,
        ['partnerTreeMetrics', 'partnerTreeMetricsProgress', 'None', 'None', 'None',
            true, 'None', false, tree_metric_args]);
    callFunctionIfInView('pacingMetrics', getTable,
        ['pacingMetrics', 'pacingMetricsProgress']);
    if (!report) {
        callFunctionIfInView('pacingAlerts', getTable,
            ['pacingAlerts', 'pacingAlertsProgress', 'None', 'None', 'None',
                true, 'None', false, pacing_alert_args, getMetricsComplete]);
        callFunctionIfInView('customChartsPartner', buildCustomCharts, ['Partner'])
    }
}

function getCountryMetrics(filterDict = null, clickElem = null, oldHtml = null,
                           allCharts = false, report = false) {
    let metrics = ['kpi'];
    filterDict = (filterDict) ? filterDict : [];
    let country_bar_args = getDataTableArgsDict(
        'generateDualBarChart', ['countryname'],
        metrics, filterDict, true, undefined, undefined, true);
    let country_metric_args = getDataTableArgsDict(
        'generateBarChart', ['campaignname', 'countryname'],
        metrics, filterDict, true, undefined, undefined, false);
    let tree_metric_args = getDataTableArgsDict('generateTreeMap',
        ['regionname', 'countryname'], ['netcost'], filterDict, true);
    callFunctionIfInView('countryBarMetrics', getTable,
        ['countryBarMetrics', 'countryBarMetricsProgress', 'None', 'None',
            'None', true, 'None', false, country_bar_args]);
    callFunctionIfInView('countrySummaryMetrics', getTable,
        ['countrySummaryMetrics', 'countrySummaryMetricsProgress', 'None',
            'None', 'None', true, 'None', false, country_metric_args]);
    if (!report) {
        callFunctionIfInView('countryTreeMap', getTable,
            ['countryTreeMap', 'countryTreeMapProgress', 'None', 'None',
                'None', true, 'None', false, tree_metric_args]);
        callFunctionIfInView('customChartsCountry', buildCustomCharts, ['Country']);
    }
}

function getTargetingMetrics(filterDict = null, clickElem = null, oldHtml = null,
                             allCharts = false, report = false) {
    let metrics = ['kpi'];
    filterDict = (filterDict) ? filterDict : [];
    let targeting_bar_args = getDataTableArgsDict(
        'generateDualBarChart', ['targetingname'],
        metrics, filterDict, true, undefined, undefined, true);
    let targeting_metric_args = getDataTableArgsDict(
        'generateBarChart', ['campaignname', 'targetingname'],
        metrics, filterDict, true, undefined, undefined, false);
    callFunctionIfInView('targetingBarMetrics', getTable,
        ['targetingBarMetrics', 'targetingBarMetricsProgress', 'None',
            'None', 'None', true, 'None', false, targeting_bar_args]);
    callFunctionIfInView('targetingSummaryMetrics', getTable,
        ['targetingSummaryMetrics', 'targetingSummaryMetricsProgress',
            'None', 'None', 'None', true, 'None', false, targeting_metric_args]);
    if (!report) {
        callFunctionIfInView('customChartsTargeting', buildCustomCharts, ['Targeting']);
    }
}

function getPlacementMetrics(filterDict = null, clickElem = null, oldHtml = null,
                             allCharts = false, report=false) {
    let metrics = ['kpi'];
    filterDict = (filterDict) ? filterDict : [];
    let placement_bar_args = getDataTableArgsDict(
        'generateDualBarChart', ['packagedescriptionname'],
        metrics, filterDict, true, undefined, undefined, true);
    let placement_metric_args = getDataTableArgsDict(
        'generateBarChart', ['campaignname', 'packagedescriptionname'],
        metrics, filterDict, true, undefined, undefined, false);
    callFunctionIfInView('placementBarMetrics', getTable,
        ['placementBarMetrics', 'placementBarMetricsProgress', 'None',
            'None', 'None', true, 'None', false, placement_bar_args]);
    callFunctionIfInView('placementSummaryMetrics', getTable,
        ['placementSummaryMetrics', 'placementSummaryMetricsProgress',
            'None', 'None', 'None', true, 'None', false, placement_metric_args]);
    if (!report) {
        callFunctionIfInView('customChartsPlacement', buildCustomCharts, ['Placement']);
    }
}

function getCreativeMetrics(filterDict = null, clickElem = null, oldHtml = null,
                            allCharts = false, report=false) {
    let metrics = ['kpi'];
    filterDict = (filterDict) ? filterDict : [];
    let creative_bar_args = getDataTableArgsDict(
        'generateDualBarChart', ['creativename'],
        metrics, filterDict, true, undefined, undefined, true);
    let creative_metric_args = getDataTableArgsDict(
        'generateBarChart', ['campaignname', 'creativename'],
        metrics, filterDict, true, undefined, undefined, false);
    callFunctionIfInView('creativeBarMetrics', getTable,
        ['creativeBarMetrics', 'creativeBarMetricsProgress', 'None',
            'None', 'None', true, 'None', false, creative_bar_args]);
    callFunctionIfInView('creativeSummaryMetrics', getTable,
        ['creativeSummaryMetrics', 'creativeSummaryMetricsProgress',
            'None', 'None', 'None', true, 'None', false, creative_metric_args]);
    if (!report) {
        callFunctionIfInView('customChartsCreative', buildCustomCharts, ['Creative']);
    }
}

function getAllChartFunctions() {
    return [getAllCharts, getPartnerMetrics, getCountryMetrics,
        getTargetingMetrics, getPlacementMetrics, getCreativeMetrics]
}

function getAllFilters() {
    let filterDict = getDates();
    let cols = ['campaignname', 'vendorname', 'countryname',
        'environmentname', 'kpiname'];
    filterDict = getMultipleFilters(cols, filterDict, 'Select');
    return filterDict
}

function applyFilterEvent() {
    loadingBtn(this);
    animateBar();
    let filterDict = getAllFilters();
    getAllCharts(filterDict, this.id, 'None', true);
    addElemRemoveLoadingBtn(this.id);
}

function getAllChartsClickEvent() {
    loadingBtn(this);
    animateBar();
    let filterDict = getAllFilters();
    getSecondaryCharts(filterDict);
    addElemRemoveLoadingBtn(this.id);
    this.setAttribute('hidden', true);
}

function getBillingTable() {
    let billingTableId = 'billingTable';
    getTable(billingTableId, billingTableId);
}

function loadDash(data, kwargs) {
    let dashId = kwargs['dashId'];
    let btnId = kwargs['btnId'];
    let chartName = dashId + 'Metrics';
    let chartType = data['chart_type'];
    let chartFunction = getChartFunctionName(chartType);
    let defaultView = data['default_view'] === 'Chart';
    let dash_metric_args = getDataTableArgsDict(
        chartFunction, JSON.parse(data['dimensions']),
        JSON.parse(data['metrics']), JSON.parse(data['chart_filters']),
        true, undefined, undefined, defaultView);
    getTable(chartName, btnId, 'None', 'None', 'None', true, 'None',
        false, dash_metric_args);
}

function viewDash(dashId, btnId) {
    let jv = document.getElementById('jinjaValues');
    let data = {
        object_name: jv.dataset['object_name'],
        object_type: jv.dataset['title'],
        object_level: jv.dataset['edit_name'],
        dashboard_id: dashId.replace('dash', '')
    };
    let kwargs = {'dashId': dashId, 'btnId': btnId};
    makeRequest('/get_dashboard_properties', 'POST', data, loadDash,
        'json', kwargs);
}

function handleSaveFail(data, kwargs) {
    let btnId = kwargs['btnId'];
    displayAlert('Could not save dashboard, please try again later',
        'warning');
    unanimateBar();
    addElemRemoveLoadingBtn(btnId)
}

function handleSave(data, kwargs) {
    let id = kwargs['dashId'];
    let btnId = kwargs['btnId'];
    let element = document.getElementById(id).firstElementChild;
    let originalColor = element.style.backgroundColor;
    element.style.backgroundColor = "#b3ecff";
    let t = setTimeout(function () {
        element.style.backgroundColor = originalColor;
    }, (2 * 1000));
    displayAlert(data['message'], data['level']);
    addElemRemoveLoadingBtn(btnId)
}

function saveDash(dashId, btnId) {
    let id = dashId.replace('dash', '');
    let jv = document.getElementById('jinjaValues');
    let form = document.getElementById('dash' + id + 'form').firstElementChild;
    let formData = new FormData(form);
    let data = {
        object_name: jv.dataset['object_name'],
        object_type: jv.dataset['title'],
        object_level: jv.dataset['edit_name'],
        dashboard_id: id,
        object_form: JSON.stringify(convertFormDataToDict(formData))
    };
    let kwargs = {'dashId': dashId, 'btnId': btnId};
    makeRequest('/save_dashboard', 'POST', data, handleSave,
        'json', kwargs, handleSaveFail);
}

function handleDelete(data, kwargs) {
    let id = kwargs['dashId'];
    let btnId = kwargs['btnId'];
    let element = document.getElementById(id + 'card');
    let originalColor = element.style.backgroundColor;
    element.style.backgroundColor = "rgba(255,0,0,0.68)";
    let t = setTimeout(function () {
        element.style.backgroundColor = originalColor;
    }, (2 * 1000));
    addElemRemoveLoadingBtn(btnId);
    element.innerHTML = '';
    displayAlert(data['message'], data['level']);
}

function deleteDash(dashId, btnId) {
    let jv = document.getElementById('jinjaValues');
    let data = {
        object_name: jv.dataset['object_name'],
        object_type: jv.dataset['title'],
        object_level: jv.dataset['edit_name'],
        dashboard_id: dashId.replace('dash', '')
    };
    let kwargs = {'dashId': dashId, 'btnId': btnId};
    makeRequest('/delete_dashboard', 'POST', data, handleDelete,
        'json', kwargs);
}

function deleteCustomChart(deleteButtonElem) {
    let dashId = deleteButtonElem.id.replace('Delete', '');
    deleteDash(dashId, deleteButtonElem.id)
}

function handleInclude(data) {
    displayAlert(data['message'], data['level']);
}

function includeCustomInReport(checkBoxElem) {
    let dashId = checkBoxElem.parentNode.id.replace(/Include|dash/g, "");
    let include = checkBoxElem.checked;
    let jv = document.getElementById('jinjaValues');
    let data = {
        object_name: jv.dataset['object_name'],
        object_type: jv.dataset['title'],
        object_level: jv.dataset['edit_name'],
        dashboard_id: dashId.replace('dash', ''),
        include: include
    };
    makeRequest('/include_dashboard_in_report', 'POST', data,
        handleInclude);
}

function setButtonEvents() {
    let curId = this.id;
    let functionName = function (curId) {
        if (curId.includes('View')) {
            return ['View', viewDash]
        } else if (curId.includes('Save')) {
            return ['Save', saveDash]
        } else if (curId.includes('Delete')) {
            return ['Delete', deleteDash]
        }
    };
    let currentFunction = functionName(curId);
    let dashId = this.id.replace(currentFunction[0], '');
    if (!curId.includes('View')) {
        loadingBtn(this);
    }
    currentFunction[1](dashId, this.id)
}

function createDash() {
    let jv = document.getElementById('jinjaValues');
    let object_name = jv.dataset['object_name'].trim();
    let url = `/processor/${object_name}/dashboard/create?render=false`;
    let form = document.getElementById('base_form_id');
    let formData = new FormData(form);
    makeRequest(url, 'POST', formData, reloadPage);
}

async function addDashModal(tab) {
    let response = await fetch(`/get_dash_form?tab=${tab}`);
    let json_response = await response.json();
    let dash_form = json_response['form_html'];
    showModalTable('addDash');
    let modal = document.getElementById('addDashmodalTable');
    let saveButton = modal.querySelector(
        '#modalTableSaveButton, #saveDashButton');
    saveButton.id = 'saveDashButton';
    saveButton.removeAttribute('onclick');
    let modalBody = modal.querySelector('[class="modal-body"]');
    modalBody.innerHTML = dash_form;
    addSelectize();
    addOnClickEvent('[id^="saveDashButton"]', createDash);
}
