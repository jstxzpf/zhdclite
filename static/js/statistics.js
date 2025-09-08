/**
 * 统计页面JavaScript功能
 */

// 全局变量
let currentCharts = {};
let currentData = {};

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    initializePage();
    setupEventListeners();
    loadInitialOverviewOnly();
});

// 初始化页面
function initializePage() {
    console.log('统计页面初始化...');

    // 设置默认时间范围
    setDefaultDateRange();

    // 加载筛选选项
    loadFilterOptions();

    // 初始化筛选信息显示
    updateFilterDisplay();
}

// 设置默认时间范围
function setDefaultDateRange() {
    const currentDate = new Date();
    const currentYear = currentDate.getFullYear();
    const currentMonth = String(currentDate.getMonth() + 1).padStart(2, '0');

    // 默认结束时间：当前月份
    const defaultEndDate = `${currentYear}-${currentMonth}`;

    // 默认起始时间：当前年份的上一年12月
    const defaultStartDate = `${currentYear - 1}-12`;

    // 设置默认值
    document.getElementById('startDate').value = defaultStartDate;
    document.getElementById('endDate').value = defaultEndDate;
}

// 设置事件监听器
function setupEventListeners() {
    // 筛选器事件
    document.getElementById('applyFilter').addEventListener('click', applyFilters);
    document.getElementById('resetFilter').addEventListener('click', resetFilters);

    // 级联筛选事件
    document.getElementById('filterTown').addEventListener('change', onTownChange);
    document.getElementById('filterVillage').addEventListener('change', onVillageChange);

    // 筛选条件变化时更新显示
    document.getElementById('startDate').addEventListener('change', updateFilterDisplay);
    document.getElementById('endDate').addEventListener('change', updateFilterDisplay);
    document.getElementById('filterTown').addEventListener('change', updateFilterDisplay);
    document.getElementById('filterVillage').addEventListener('change', updateFilterDisplay);
    document.getElementById('filterHousehold').addEventListener('change', updateFilterDisplay);

    // 清除单个筛选条件按钮事件
    document.querySelectorAll('.filter-clear-btn').forEach(btn => {
        btn.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const target = this.getAttribute('data-target');
            clearSingleFilter(target);
        });
    });

    // 清空所有筛选按钮事件
    document.getElementById('clearAllFilters').addEventListener('click', clearAllFilters);

    // 选项卡切换事件
    document.querySelectorAll('.tab-item').forEach(tab => {
        tab.addEventListener('click', function() {
            switchTab(this.dataset.tab);
        });
    });

    // 初始化深度分析事件监听器
    initializeAnalysisEventListeners();
}

// 加载筛选选项
async function loadFilterOptions() {
    try {
        // 并行加载不同的筛选选项
        const [filtersResponse, townsResponse] = await Promise.all([
            fetch('/api/statistics/available_filters'),
            fetch('/api/towns')
        ]);
        
        const filtersData = await filtersResponse.json();
        const townsData = await townsResponse.json();
        
        if (filtersData.success && townsData.success) {
            initializeDateFilters(filtersData.data);
            populateTownsFilter(townsData.data);
        } else {
            showMessage('error', '加载筛选选项失败');
        }
    } catch (error) {
        console.error('加载筛选选项失败:', error);
        showMessage('error', '加载筛选选项失败');
    }
}

// 初始化时间范围筛选器
function initializeDateFilters(options) {
    // 使用统一的默认时间范围设置
    setDefaultDateRange();

    // 设置最小和最大值范围
    if (options.years && options.years.length > 0) {
        const minYear = Math.min(...options.years.filter(y => !isNaN(y)));
        const maxYear = Math.max(...options.years.filter(y => !isNaN(y)));

        const startDateInput = document.getElementById('startDate');
        const endDateInput = document.getElementById('endDate');

        startDateInput.min = `${minYear}-01`;
        startDateInput.max = `${maxYear}-12`;
        endDateInput.min = `${minYear}-01`;
        endDateInput.max = `${maxYear}-12`;
    }
}

// 填充乡镇筛选选项
function populateTownsFilter(towns) {
    const townSelect = document.getElementById('filterTown');
    towns.forEach(town => {
        if (town && town.trim() !== '') {
            townSelect.add(new Option(town, town));
        }
    });
}

// 加载初始概览数据（不包含详细统计）
async function loadInitialOverviewOnly() {
    showLoading('正在加载概览数据...');

    try {
        // 只加载总体概览
        await loadOverviewData();

        // 显示分户统计的提示信息
        showHouseholdEmptyState();

    } catch (error) {
        console.error('加载概览数据失败:', error);
        showMessage('error', '加载概览数据失败');
    } finally {
        hideLoading();
    }
}

// 显示分户统计的空状态提示
function showHouseholdEmptyState() {
    const householdTab = document.getElementById('household-tab');
    const statisticsContent = householdTab.querySelector('.statistics-content');

    // 创建提示信息
    const emptyStateHtml = `
        <div class="empty-state" id="householdEmptyState">
            <div class="empty-state-icon">
                <i class="fas fa-chart-bar"></i>
            </div>
            <div class="empty-state-content">
                <h3>分户统计分析</h3>
                <p>请设置筛选条件并点击"应用筛选"按钮查看分户统计数据</p>
                <div class="empty-state-tips">
                    <p><i class="fas fa-info-circle"></i> 您可以通过以下方式筛选数据：</p>
                    <ul>
                        <li>选择时间范围（开始时间 - 结束时间）</li>
                        <li>选择特定乡镇或村庄</li>
                        <li>输入特定户代码</li>
                    </ul>
                </div>
            </div>
        </div>
    `;

    // 隐藏图表和表格，显示提示信息
    const chartContainer = statisticsContent.querySelector('.chart-container');
    const tableContainer = statisticsContent.querySelector('.table-container');

    chartContainer.style.display = 'none';
    tableContainer.style.display = 'none';

    // 插入提示信息
    statisticsContent.insertAdjacentHTML('afterbegin', emptyStateHtml);
}

// 隐藏分户统计的空状态提示
function hideHouseholdEmptyState() {
    const emptyState = document.getElementById('householdEmptyState');
    if (emptyState) {
        emptyState.remove();
    }

    // 显示图表和表格容器
    const householdTab = document.getElementById('household-tab');
    const statisticsContent = householdTab.querySelector('.statistics-content');
    const chartContainer = statisticsContent.querySelector('.chart-container');
    const tableContainer = statisticsContent.querySelector('.table-container');

    chartContainer.style.display = 'block';
    tableContainer.style.display = 'block';
}

// 更新筛选条件显示
function updateFilterDisplay() {
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    const town = document.getElementById('filterTown').value;
    const village = document.getElementById('filterVillage').value;
    const household = document.getElementById('filterHousehold').value;

    // 格式化日期显示
    const formatDate = (dateStr) => {
        if (!dateStr) return '';
        const [year, month] = dateStr.split('-');
        return `${year}年${month}月`;
    };

    // 更新时间范围显示
    let timeRangeText = '未选择';
    if (startDate && endDate) {
        timeRangeText = `${formatDate(startDate)} 至 ${formatDate(endDate)}`;
    } else if (startDate) {
        timeRangeText = `从 ${formatDate(startDate)} 开始`;
    } else if (endDate) {
        timeRangeText = `到 ${formatDate(endDate)} 结束`;
    }
    document.getElementById('displayTimeRange').textContent = timeRangeText;

    // 更新乡镇显示
    const townText = town ?
        (document.getElementById('filterTown').selectedOptions[0]?.text || town) : '全部乡镇';
    document.getElementById('displayTown').textContent = townText;

    // 更新村庄显示
    const villageText = village ?
        (document.getElementById('filterVillage').selectedOptions[0]?.text || village) : '全部村庄';
    document.getElementById('displayVillage').textContent = villageText;

    // 更新户代码显示
    const householdText = household ?
        (document.getElementById('filterHousehold').selectedOptions[0]?.text || household) : '全部户';
    document.getElementById('displayHousehold').textContent = householdText;

    // 更新清除按钮的显示状态
    updateClearButtonsVisibility();
}

// 更新清除按钮的显示状态
function updateClearButtonsVisibility() {
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    const town = document.getElementById('filterTown').value;
    const village = document.getElementById('filterVillage').value;
    const household = document.getElementById('filterHousehold').value;

    // 控制各个清除按钮的显示
    document.querySelector('[data-target="time"]').style.display =
        (startDate || endDate) ? 'inline-block' : 'none';
    document.querySelector('[data-target="town"]').style.display =
        town ? 'inline-block' : 'none';
    document.querySelector('[data-target="village"]').style.display =
        village ? 'inline-block' : 'none';
    document.querySelector('[data-target="household"]').style.display =
        household ? 'inline-block' : 'none';

    // 控制"清空所有筛选"按钮的显示
    const hasAnyFilter = startDate || endDate || town || village || household;
    document.getElementById('clearAllFilters').style.display =
        hasAnyFilter ? 'inline-block' : 'none';
}

// 清除单个筛选条件
function clearSingleFilter(target) {
    switch (target) {
        case 'time':
            document.getElementById('startDate').value = '';
            document.getElementById('endDate').value = '';
            break;
        case 'town':
            document.getElementById('filterTown').value = '';
            // 清除乡镇时也要清除村庄和户代码
            document.getElementById('filterVillage').innerHTML = '<option value="">全部村庄</option>';
            document.getElementById('filterHousehold').innerHTML = '<option value="">全部户</option>';
            break;
        case 'village':
            document.getElementById('filterVillage').value = '';
            // 清除村庄时也要清除户代码
            document.getElementById('filterHousehold').innerHTML = '<option value="">全部户</option>';
            break;
        case 'household':
            document.getElementById('filterHousehold').value = '';
            break;
    }
    updateFilterDisplay();
}

// 清空所有筛选条件
function clearAllFilters() {
    document.getElementById('startDate').value = '';
    document.getElementById('endDate').value = '';
    document.getElementById('filterTown').value = '';
    document.getElementById('filterVillage').innerHTML = '<option value="">全部村庄</option>';
    document.getElementById('filterHousehold').innerHTML = '<option value="">全部户</option>';

    updateFilterDisplay();
    showMessage('info', '已清空所有筛选条件');
}

// 加载总体概览数据
async function loadOverviewData() {
    try {
        const params = getFilterParams();
        const url = params ? `/api/statistics/overview?${params}` : '/api/statistics/overview';
        const response = await fetch(url);
        const data = await response.json();

        if (data.success) {
            updateOverviewDisplay(data.data);
        } else {
            throw new Error(data.message);
        }
    } catch (error) {
        console.error('加载概览数据失败:', error);
        throw error;
    }
}

// 更新概览显示
function updateOverviewDisplay(data) {
    document.getElementById('totalRecords').textContent = formatNumber(data.total_records);
    document.getElementById('totalHouseholds').textContent = formatNumber(data.total_households);
    document.getElementById('totalMonths').textContent = formatNumber(data.total_months);
    document.getElementById('totalIncome').textContent = formatCurrency(data.total_income);
    document.getElementById('totalExpenditure').textContent = formatCurrency(data.total_expenditure);
    
    // 计算编码完成率
    const codingRate = data.total_records > 0 ? 
        ((data.coded_records / data.total_records) * 100).toFixed(1) + '%' : '0%';
    document.getElementById('codingRate').textContent = codingRate;
}

// 应用筛选器
async function applyFilters() {
    const activeTab = document.querySelector('.tab-item.active').dataset.tab;

    showLoading('正在应用筛选条件...');

    try {
        // 首先更新总体概览数据
        await loadOverviewData();

        // 然后更新当前选项卡的数据
        switch (activeTab) {
            case 'household':
                // 隐藏空状态提示，显示数据区域
                hideHouseholdEmptyState();
                await loadHouseholdStatistics();
                break;
            case 'town':
                await loadTownStatistics();
                break;
            case 'month':
                await loadMonthStatistics();
                break;
            case 'consumption':
                await loadConsumptionStatistics();
                break;
            case 'missing':
                await loadMissingAnalysis();
                break;
            case 'analysis':
                updateAnalysisScope();
                break;
        }

        showMessage('success', '筛选条件应用成功');
    } catch (error) {
        console.error('应用筛选失败:', error);
        showMessage('error', '应用筛选失败');
    } finally {
        hideLoading();
    }
}

// 重置筛选器
function resetFilters() {
    document.getElementById('startDate').value = '';
    document.getElementById('endDate').value = '';
    document.getElementById('filterTown').value = '';
    document.getElementById('filterVillage').innerHTML = '<option value="">全部村庄</option>';
    document.getElementById('filterHousehold').innerHTML = '<option value="">全部户</option>';

    // 重新设置默认时间范围
    setDefaultDateRange();

    // 更新筛选条件显示
    updateFilterDisplay();

    applyFilters();
}

// 切换选项卡
function switchTab(tabName) {
    // 更新选项卡状态
    document.querySelectorAll('.tab-item').forEach(tab => {
        tab.classList.remove('active');
    });
    document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');
    
    // 更新内容区域
    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.classList.remove('active');
    });
    document.getElementById(`${tabName}-tab`).classList.add('active');
    
    // 加载对应数据
    loadTabData(tabName);
}

// 加载选项卡数据
async function loadTabData(tabName) {
    // 对于分户统计和深度分析，不自动加载数据
    if (tabName === 'household') {
        showHouseholdEmptyState();
        return;
    }

    if (tabName === 'analysis') {
        updateAnalysisScope();
        return;
    }

    showLoading('正在加载数据...');

    try {
        switch (tabName) {
            case 'town':
                await loadTownStatistics();
                break;
            case 'month':
                await loadMonthStatistics();
                break;
            case 'consumption':
                await loadConsumptionStatistics();
                break;
            case 'missing':
                await loadMissingAnalysis();
                break;
        }
    } catch (error) {
        console.error('加载选项卡数据失败:', error);
        showMessage('error', '加载数据失败');
    } finally {
        hideLoading();
    }
}

// 加载分户统计数据
async function loadHouseholdStatistics() {
    try {
        const params = getFilterParams();
        const response = await fetch(`/api/statistics/by_household?${params}`);
        const data = await response.json();
        
        if (data.success) {
            currentData.household = data.data;
            updateHouseholdTable(data.data);
            updateHouseholdChart(data.data);
        } else {
            throw new Error(data.message);
        }
    } catch (error) {
        console.error('加载分户统计失败:', error);
        throw error;
    }
}

// 加载分乡镇统计数据
async function loadTownStatistics() {
    try {
        const params = getFilterParams();
        const response = await fetch(`/api/statistics/by_town?${params}`);
        const data = await response.json();
        
        if (data.success) {
            currentData.town = data.data;
            updateTownTable(data.data);
            updateTownChart(data.data);
        } else {
            throw new Error(data.message);
        }
    } catch (error) {
        console.error('加载分乡镇统计失败:', error);
        throw error;
    }
}

// 加载分月统计数据
async function loadMonthStatistics() {
    try {
        const params = getFilterParams();
        const response = await fetch(`/api/statistics/by_month?${params}`);
        const data = await response.json();
        
        if (data.success) {
            currentData.month = data.data;
            updateMonthTable(data.data);
            updateMonthChart(data.data);
        } else {
            throw new Error(data.message);
        }
    } catch (error) {
        console.error('加载分月统计失败:', error);
        throw error;
    }
}

// 加载消费结构数据
async function loadConsumptionStatistics() {
    try {
        const params = getFilterParams();
        const response = await fetch(`/api/statistics/consumption_structure?${params}`);
        const data = await response.json();
        
        if (data.success) {
            currentData.consumption = data.data;
            updateConsumptionTable(data.data);
            updateConsumptionChart(data.data);
        } else {
            throw new Error(data.message);
        }
    } catch (error) {
        console.error('加载消费结构统计失败:', error);
        throw error;
    }
}

// 加载漏记账分析数据
async function loadMissingAnalysis() {
    showLoading('正在加载漏记账分析...');

    try {
        await analyzeMissingDays();
    } catch (error) {
        console.error('加载漏记账分析失败:', error);
        showMessage('error', '加载漏记账分析失败');
    } finally {
        hideLoading();
    }
}

// 分析漏记账天数
async function analyzeMissingDays() {
    // 直接使用数据筛选界面的条件
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;

    if (!startDate || !endDate) {
        console.warn('时间范围未设置，无法进行漏记账分析');
        return;
    }

    try {
        // 从时间段范围中提取年月信息
        const [startYear, startMonth] = startDate.split('-');
        const [endYear, endMonth] = endDate.split('-');

        // 获取所有筛选条件，确保完整传递
        const townFilter = document.getElementById('filterTown').value;
        const villageFilter = document.getElementById('filterVillage').value;
        const householdFilter = document.getElementById('filterHousehold').value;

        // 使用时间区间参数而不是单个月份
        let params = `start_year=${startYear}&start_month=${startMonth}&end_year=${endYear}&end_month=${endMonth}`;

        // 添加乡镇筛选
        if (townFilter) {
            params += `&town=${encodeURIComponent(townFilter)}`;
        }

        // 添加村庄筛选
        if (villageFilter) {
            params += `&village=${encodeURIComponent(villageFilter)}`;
        }

        // 添加户代码筛选
        if (householdFilter) {
            params += `&household=${encodeURIComponent(householdFilter)}`;
        }

        console.log('漏记账分析请求参数:', params);

        const response = await fetch(`/api/statistics/missing_days?${params}`);
        const data = await response.json();

        if (data.success) {
            currentData.missing = data.data;
            updateMissingTable(data.data);
            updateMissingChart(data.data);

            console.log(`漏记账分析完成，共分析 ${data.data.length} 户`);
        } else {
            throw new Error(data.message);
        }
    } catch (error) {
        console.error('分析漏记账失败:', error);
        throw error; // 重新抛出错误，让上层处理
    }
}

// 获取筛选参数
function getFilterParams() {
    const params = new URLSearchParams();

    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    const town = document.getElementById('filterTown').value;
    const village = document.getElementById('filterVillage').value;
    const household = document.getElementById('filterHousehold').value;

    // 处理开始时间
    if (startDate) {
        const [startYear, startMonth] = startDate.split('-');
        if (startYear && startMonth) {
            params.append('start_year', startYear);
            params.append('start_month', startMonth);
        }
    }

    // 处理结束时间
    if (endDate) {
        const [endYear, endMonth] = endDate.split('-');
        if (endYear && endMonth) {
            params.append('end_year', endYear);
            params.append('end_month', endMonth);
        }
    }

    // 验证并添加乡镇参数
    if (town && town.trim() !== '' && town !== 'null' && town !== 'undefined') {
        params.append('town', town);
    }

    // 验证并添加村庄参数
    if (village && village.trim() !== '' && village !== 'null' && village !== 'undefined') {
        params.append('village', village);
    }

    // 验证并添加户代码参数
    if (household && household.trim() !== '' && household !== 'null' && household !== 'undefined') {
        params.append('household', household);
    }

    return params.toString();
}

// 乡镇变化时的级联筛选
async function onTownChange() {
    const townSelect = document.getElementById('filterTown');
    const villageSelect = document.getElementById('filterVillage');
    const householdSelect = document.getElementById('filterHousehold');

    // 清空村庄和户代码选项
    villageSelect.innerHTML = '<option value="">全部村庄</option>';
    householdSelect.innerHTML = '<option value="">全部户</option>';

    const selectedTown = townSelect.value;
    if (!selectedTown) {
        return;
    }

    try {
        // 使用新的API端点获取村庄列表
        const response = await fetch(`/api/villages?town=${encodeURIComponent(selectedTown)}`);
        const data = await response.json();
        
        if (data.success) {
            data.data.forEach(village => {
                if (village && village.name && village.code) {
                    villageSelect.add(new Option(village.name, village.code));
                }
            });
        }
    } catch (error) {
        console.error('加载村庄列表失败:', error);
        showMessage('error', '加载村庄列表失败');
    }
}

// 村庄变化时的级联筛选
function onVillageChange() {
    const householdSelect = document.getElementById('filterHousehold');
    // 清空户代码选项 - 可以通过现有的available_filters接口获取
    householdSelect.innerHTML = '<option value="">全部户</option>';
    
    const townSelect = document.getElementById('filterTown');
    const villageSelect = document.getElementById('filterVillage');
    const selectedTown = townSelect.value;
    const selectedVillage = villageSelect.value;
    
    // 如果有选择的村庄，通过现有API获取户代码
    if (selectedVillage) {
        loadHouseholdsByVillage(selectedVillage);
    } else if (selectedTown) {
        loadVillagesAndHouseholds(selectedTown);
    }
}

// 根据乡镇加载村庄和户代码
async function loadVillagesAndHouseholds(townName) {
    try {
        const response = await fetch(`/api/statistics/available_filters?town=${encodeURIComponent(townName)}`);
        const data = await response.json();

        if (data.success) {
            const villageSelect = document.getElementById('filterVillage');
            const householdSelect = document.getElementById('filterHousehold');

            // 填充村庄选项
            if (data.data.villages) {
                data.data.villages.forEach(village => {
                    if (village && village.name && village.code) {
                        villageSelect.add(new Option(village.name, village.code));
                    }
                });
            }

            // 填充户代码选项
            if (data.data.households) {
                data.data.households.forEach(household => {
                    if (household && household.code && household.name) {
                        const displayText = `${household.code} - ${household.name}`;
                        householdSelect.add(new Option(displayText, household.code));
                    }
                });
            }
        }
    } catch (error) {
        console.error('加载村庄和户代码失败:', error);
    }
}

// 根据村庄加载户代码
async function loadHouseholdsByVillage(villageCode) {
    try {
        const response = await fetch(`/api/statistics/available_filters?village=${villageCode}`);
        const data = await response.json();

        if (data.success && data.data.households) {
            const householdSelect = document.getElementById('filterHousehold');

            data.data.households.forEach(household => {
                if (household && household.code && household.name) {
                    const displayText = `${household.code} - ${household.name}`;
                    householdSelect.add(new Option(displayText, household.code));
                }
            });
        }
    } catch (error) {
        console.error('加载户代码失败:', error);
    }
}

// 更新分户统计表格
function updateHouseholdTable(data) {
    const tbody = document.querySelector('#householdTable tbody');
    tbody.innerHTML = '';

    data.forEach(row => {
        const codingRate = row.记账笔数 > 0 ?
            ((row.已编码笔数 / row.记账笔数) * 100).toFixed(1) + '%' : '0%';

        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${row.户代码}</td>
            <td>${row.户主姓名}</td>
            <td>${row.年份}</td>
            <td>${row.月份}</td>
            <td>${row.记账笔数}</td>
            <td>${row.收入笔数}</td>
            <td>${row.支出笔数}</td>
            <td>${formatCurrency(row.收入总额)}</td>
            <td>${formatCurrency(row.支出总额)}</td>
            <td>${codingRate}</td>
        `;
        tbody.appendChild(tr);
    });
}

// 更新分乡镇统计表格
function updateTownTable(data) {
    const tbody = document.querySelector('#townTable tbody');
    tbody.innerHTML = '';

    data.forEach(row => {
        const codingRate = row.记账笔数 > 0 ?
            ((row.已编码笔数 / row.记账笔数) * 100).toFixed(1) + '%' : '0%';

        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${row.乡镇名称}</td>
            <td>${row.户数}</td>
            <td>${row.记账笔数}</td>
            <td>${row.收入笔数}</td>
            <td>${row.支出笔数}</td>
            <td>${formatCurrency(row.收入总额)}</td>
            <td>${formatCurrency(row.支出总额)}</td>
            <td>${codingRate}</td>
        `;
        tbody.appendChild(tr);
    });
}

// 更新分月统计表格
function updateMonthTable(data) {
    const tbody = document.querySelector('#monthTable tbody');
    tbody.innerHTML = '';

    data.forEach(row => {
        const codingRate = row.记账笔数 > 0 ?
            ((row.已编码笔数 / row.记账笔数) * 100).toFixed(1) + '%' : '0%';

        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${row.年份}</td>
            <td>${row.月份}</td>
            <td>${row.户数}</td>
            <td>${row.记账笔数}</td>
            <td>${row.收入笔数}</td>
            <td>${row.支出笔数}</td>
            <td>${formatCurrency(row.收入总额)}</td>
            <td>${formatCurrency(row.支出总额)}</td>
            <td>${codingRate}</td>
        `;
        tbody.appendChild(tr);
    });
}

// 更新消费结构表格
function updateConsumptionTable(data) {
    const tbody = document.querySelector('#consumptionTable tbody');
    tbody.innerHTML = '';

    data.forEach(row => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${row.消费类别 || '未知'}</td>
            <td>${row.编码}</td>
            <td>${row.记账笔数}</td>
            <td>${formatCurrency(row.总金额)}</td>
            <td>${formatCurrency(row.平均金额)}</td>
            <td>${row.涉及户数}</td>
        `;
        tbody.appendChild(tr);
    });
}

// 更新漏记账表格
function updateMissingTable(data) {
    const tbody = document.querySelector('#missingTable tbody');
    tbody.innerHTML = '';

    data.forEach(row => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${row.户代码}</td>
            <td>${row.户主姓名}</td>
            <td>${row.记账天数}</td>
            <td class="${row.漏记账天数 > 5 ? 'warning' : ''}">${row.漏记账天数}</td>
            <td>${row.总记账笔数}</td>
            <td>${formatChineseDate(row.首次记账日期)}</td>
            <td>${formatChineseDate(row.最后记账日期)}</td>
        `;
        tbody.appendChild(tr);
    });
}

// 更新分户统计图表
function updateHouseholdChart(data) {
    const ctx = document.getElementById('householdChart').getContext('2d');

    // 销毁现有图表
    if (currentCharts.household) {
        currentCharts.household.destroy();
    }

    // 取前10户数据用于图表显示
    const chartData = data.slice(0, 10);

    currentCharts.household = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: chartData.map(row => row.户主姓名),
            datasets: [{
                label: '收入总额',
                data: chartData.map(row => row.收入总额),
                backgroundColor: 'rgba(54, 162, 235, 0.8)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1
            }, {
                label: '支出总额',
                data: chartData.map(row => row.支出总额),
                backgroundColor: 'rgba(255, 99, 132, 0.8)',
                borderColor: 'rgba(255, 99, 132, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return formatCurrency(value);
                        }
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: '分户收支统计 (前10户)'
                },
                legend: {
                    display: true,
                    position: 'top'
                }
            }
        }
    });
}

// 更新分乡镇统计图表
function updateTownChart(data) {
    const ctx = document.getElementById('townChart').getContext('2d');

    // 销毁现有图表
    if (currentCharts.town) {
        currentCharts.town.destroy();
    }

    currentCharts.town = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: data.map(row => row.乡镇名称),
            datasets: [{
                label: '记账笔数',
                data: data.map(row => row.记账笔数),
                backgroundColor: [
                    '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0',
                    '#9966FF', '#FF9F40', '#FF6384', '#C9CBCF',
                    '#4BC0C0', '#FF6384', '#36A2EB', '#FFCE56',
                    '#9966FF', '#FF9F40', '#C9CBCF', '#4BC0C0'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: '各乡镇记账笔数分布'
                },
                legend: {
                    display: true,
                    position: 'right'
                }
            }
        }
    });
}

// 更新分月统计图表
function updateMonthChart(data) {
    const ctx = document.getElementById('monthChart').getContext('2d');

    // 销毁现有图表
    if (currentCharts.month) {
        currentCharts.month.destroy();
    }

    currentCharts.month = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(row => `${row.年份}年${row.月份}月`),
            datasets: [{
                label: '收入总额',
                data: data.map(row => row.收入总额),
                borderColor: 'rgba(54, 162, 235, 1)',
                backgroundColor: 'rgba(54, 162, 235, 0.1)',
                borderWidth: 2,
                fill: true
            }, {
                label: '支出总额',
                data: data.map(row => row.支出总额),
                borderColor: 'rgba(255, 99, 132, 1)',
                backgroundColor: 'rgba(255, 99, 132, 0.1)',
                borderWidth: 2,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return formatCurrency(value);
                        }
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: '月度收支趋势'
                },
                legend: {
                    display: true,
                    position: 'top'
                }
            }
        }
    });
}

// 更新消费结构图表
function updateConsumptionChart(data) {
    const ctx = document.getElementById('consumptionChart').getContext('2d');

    // 销毁现有图表
    if (currentCharts.consumption) {
        currentCharts.consumption.destroy();
    }

    // 取前10项消费类别
    const chartData = data.slice(0, 10);

    currentCharts.consumption = new Chart(ctx, {
        type: 'pie',
        data: {
            labels: chartData.map(row => row.消费类别 || '未知'),
            datasets: [{
                label: '消费金额',
                data: chartData.map(row => row.总金额),
                backgroundColor: [
                    '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0',
                    '#9966FF', '#FF9F40', '#FF6384', '#C9CBCF',
                    '#4BC0C0', '#FF6384'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: '消费结构分布 (前10项)'
                },
                legend: {
                    display: true,
                    position: 'right'
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = formatCurrency(context.parsed);
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = ((context.parsed / total) * 100).toFixed(1);
                            return `${label}: ${value} (${percentage}%)`;
                        }
                    }
                }
            }
        }
    });
}

// 更新漏记账图表
function updateMissingChart(data) {
    const ctx = document.getElementById('missingChart').getContext('2d');

    // 销毁现有图表
    if (currentCharts.missing) {
        currentCharts.missing.destroy();
    }

    // 按漏记账天数分组统计
    const missingGroups = {
        '0天': 0,
        '1-3天': 0,
        '4-7天': 0,
        '8-15天': 0,
        '15天以上': 0
    };

    data.forEach(row => {
        const missingDays = row.漏记账天数;
        if (missingDays === 0) {
            missingGroups['0天']++;
        } else if (missingDays <= 3) {
            missingGroups['1-3天']++;
        } else if (missingDays <= 7) {
            missingGroups['4-7天']++;
        } else if (missingDays <= 15) {
            missingGroups['8-15天']++;
        } else {
            missingGroups['15天以上']++;
        }
    });

    currentCharts.missing = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: Object.keys(missingGroups),
            datasets: [{
                label: '户数',
                data: Object.values(missingGroups),
                backgroundColor: [
                    'rgba(75, 192, 192, 0.8)',
                    'rgba(54, 162, 235, 0.8)',
                    'rgba(255, 206, 86, 0.8)',
                    'rgba(255, 159, 64, 0.8)',
                    'rgba(255, 99, 132, 0.8)'
                ],
                borderColor: [
                    'rgba(75, 192, 192, 1)',
                    'rgba(54, 162, 235, 1)',
                    'rgba(255, 206, 86, 1)',
                    'rgba(255, 159, 64, 1)',
                    'rgba(255, 99, 132, 1)'
                ],
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        stepSize: 1
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: '漏记账天数分布'
                },
                legend: {
                    display: false
                }
            }
        }
    });
}

// 工具函数：格式化数字
function formatNumber(num) {
    if (num === null || num === undefined) return '0';
    return num.toLocaleString();
}

// 工具函数：格式化日期为中文格式
// 支持多种日期格式：RFC 1123、ISO 8601、时间戳、Date对象、YYYY-MM-DD、YYYY-MM
function formatChineseDate(dateInput, useUTC = true) {
    if (!dateInput || dateInput === 'N/A') return 'N/A';

    // 已是 Date 对象
    if (dateInput instanceof Date) {
        if (isNaN(dateInput)) return 'N/A';
        return formatFromDate(dateInput, useUTC);
    }

    // 纯数字（时间戳：秒或毫秒）
    if (typeof dateInput === 'number' || (typeof dateInput === 'string' && /^\d+$/.test(dateInput))) {
        let ts = Number(dateInput);
        if (ts < 1e12) ts *= 1000; // 可能是秒，转毫秒
        const d = new Date(ts);
        return isNaN(d) ? 'N/A' : formatFromDate(d, useUTC);
    }

    if (typeof dateInput === 'string') {
        // 快速通道：YYYY-MM-DD
        let m = dateInput.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (m) return `${m[1]}年${m[2]}月${m[3]}日`;

        // 快速通道：YYYY-MM
        m = dateInput.match(/^(\d{4})-(\d{2})$/);
        if (m) return `${m[1]}年${m[2]}月`;

        // 通用解析：支持 RFC 1123、ISO 8601 等
        const d = new Date(dateInput);
        return isNaN(d) ? dateInput : formatFromDate(d, useUTC);
    }

    // 其它类型直接返回
    return String(dateInput);

    function formatFromDate(d, utc) {
        const y = utc ? d.getUTCFullYear() : d.getFullYear();
        const m = (utc ? d.getUTCMonth() : d.getMonth()) + 1;
        const day = utc ? d.getUTCDate() : d.getDate();
        const mm = String(m).padStart(2, '0');
        const dd = String(day).padStart(2, '0');
        return `${y}年${mm}月${dd}日`;
    }
}

// 工具函数：格式化货币
function formatCurrency(amount) {
    if (amount === null || amount === undefined) return '¥0.00';
    return '¥' + parseFloat(amount).toLocaleString('zh-CN', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

// 显示加载状态
function showLoading(message = '正在加载...') {
    const overlay = document.getElementById('loadingOverlay');
    const text = document.getElementById('loadingText');
    text.textContent = message;
    overlay.style.display = 'flex';
}

// 隐藏加载状态
function hideLoading() {
    const overlay = document.getElementById('loadingOverlay');
    overlay.style.display = 'none';
}

// 显示消息
function showMessage(type, message) {
    const container = document.getElementById('messageContainer');
    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${type}`;
    messageDiv.innerHTML = `
        <i class="fas fa-${type === 'success' ? 'check-circle' :
                           type === 'error' ? 'exclamation-circle' :
                           type === 'warning' ? 'exclamation-triangle' : 'info-circle'}"></i>
        <span>${message}</span>
        <button class="message-close" onclick="this.parentElement.remove()">×</button>
    `;

    container.appendChild(messageDiv);

    // 自动移除消息
    setTimeout(() => {
        if (messageDiv.parentElement) {
            messageDiv.remove();
        }
    }, 5000);
}

// ==================== 深度分析功能 ====================

// 全局变量
let analysisInProgress = false;
let analysisAborted = false;

// 更新分析范围显示
function updateAnalysisScope() {
    const params = getFilterParams();
    const urlParams = new URLSearchParams(params);

    let scope = '';
    let canAnalyze = false;

    // 检查筛选条件
    const household = urlParams.get('household');
    const village = urlParams.get('village');
    const town = urlParams.get('town');

    if (household) {
        // 单户分析模式
        const householdSelect = document.getElementById('filterHousehold');
        const selectedOption = householdSelect.options[householdSelect.selectedIndex];
        const householdName = selectedOption ? selectedOption.text.split(' - ')[1] || '未知' : '未知';
        scope = `单户分析：${household} (${householdName})`;
        canAnalyze = true;
    } else if (village) {
        // 村庄批量分析模式
        scope = `村庄批量分析：${village}`;
        canAnalyze = true;
    } else if (town) {
        // 乡镇批量分析模式
        scope = `乡镇批量分析：${town}`;
        canAnalyze = true;
    } else {
        scope = '请先设置筛选条件（户代码、村庄或乡镇）';
        canAnalyze = false;
    }

    // 更新界面
    document.getElementById('analysisScope').textContent = scope;
    document.getElementById('startAnalysis').disabled = !canAnalyze;

    // 重置分析状态
    resetAnalysisState();
}

// 重置分析状态
function resetAnalysisState(hideResults = true) {
    document.getElementById('analysisStatus').textContent = '等待开始分析';
    document.getElementById('analysisProgress').style.display = 'none';

    // 只有在需要隐藏结果时才隐藏，默认为true保持向后兼容
    if (hideResults) {
        document.getElementById('analysisResults').style.display = 'none';
    }

    document.getElementById('startAnalysis').style.display = 'inline-block';
    document.getElementById('stopAnalysis').style.display = 'none';
    analysisInProgress = false;
    analysisAborted = false;
}

// 开始深度分析
async function startDeepAnalysis() {
    if (analysisInProgress) {
        return;
    }

    analysisInProgress = true;
    analysisAborted = false;

    // 更新界面状态
    document.getElementById('startAnalysis').style.display = 'none';
    document.getElementById('stopAnalysis').style.display = 'inline-block';
    document.getElementById('analysisProgress').style.display = 'block';
    document.getElementById('analysisResults').style.display = 'none';

    let analysisSuccessful = false;

    try {
        const params = getFilterParams();
        const urlParams = new URLSearchParams(params);

        const household = urlParams.get('household');
        const village = urlParams.get('village');
        const town = urlParams.get('town');

        if (household) {
            // 单户分析
            await performSingleHouseholdAnalysis(household, params);
            analysisSuccessful = true;
        } else if (village || town) {
            // 批量分析
            await performBatchAnalysis(town, village, params);
            analysisSuccessful = true;
        }

    } catch (error) {
        console.error('深度分析失败:', error);
        if (!analysisAborted) {
            showMessage('error', '深度分析失败: ' + error.message);
        }
        analysisSuccessful = false;
    } finally {
        if (!analysisAborted) {
            // 如果分析成功，不隐藏结果；如果失败，隐藏结果
            resetAnalysisState(!analysisSuccessful);
        }
    }
}

// 停止分析
function stopAnalysis() {
    analysisAborted = true;
    document.getElementById('analysisStatus').textContent = '正在停止分析...';
    showMessage('warning', '分析已停止');
    resetAnalysisState();
}

// 执行单户分析
async function performSingleHouseholdAnalysis(householdCode, params) {
    updateAnalysisProgress(10, '准备分析数据...');

    if (analysisAborted) return;

    // 构建请求数据
    const urlParams = new URLSearchParams(params);
    const requestData = {
        household_code: householdCode
    };

    // 添加时间范围参数
    const startYear = urlParams.get('start_year');
    const startMonth = urlParams.get('start_month');
    const endYear = urlParams.get('end_year');
    const endMonth = urlParams.get('end_month');

    if (startYear && startMonth) {
        requestData.start_year = startYear;
        requestData.start_month = startMonth;
    }

    if (endYear && endMonth) {
        requestData.end_year = endYear;
        requestData.end_month = endMonth;
    }

    updateAnalysisProgress(30, '正在分析消费画像...');

    if (analysisAborted) return;

    // 调用单户分析API
    const response = await fetch('/api/household-analysis/single', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestData)
    });

    updateAnalysisProgress(70, '正在处理分析结果...');

    if (analysisAborted) return;

    const result = await response.json();

    if (result.success) {
        console.log('API call successful, result.data:', result.data);
        updateAnalysisProgress(100, '分析完成');
        displaySingleAnalysisResult(result.data);
        showMessage('success', '单户深度分析完成');
    } else {
        console.error('API call failed:', result.message);
        throw new Error(result.message || '分析失败');
    }
}

// 执行批量分析
async function performBatchAnalysis(townName, villageName, params) {
    updateAnalysisProgress(10, '准备批量分析...');

    if (analysisAborted) return;

    // 构建请求数据
    const urlParams = new URLSearchParams(params);
    const requestData = {};

    if (townName) {
        requestData.town_name = townName;
    }
    if (villageName) {
        requestData.village_name = villageName;
    }

    // 添加时间范围参数
    const startYear = urlParams.get('start_year');
    const startMonth = urlParams.get('start_month');
    const endYear = urlParams.get('end_year');
    const endMonth = urlParams.get('end_month');

    if (startYear && startMonth) {
        requestData.start_year = startYear;
        requestData.start_month = startMonth;
    }

    if (endYear && endMonth) {
        requestData.end_year = endYear;
        requestData.end_month = endMonth;
    }

    updateAnalysisProgress(30, '正在批量分析农户数据...');

    if (analysisAborted) return;

    // 调用区域分析API
    const response = await fetch('/api/household-analysis/area', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestData)
    });

    updateAnalysisProgress(80, '正在汇总分析结果...');

    if (analysisAborted) return;

    const result = await response.json();

    if (result.success) {
        updateAnalysisProgress(100, '批量分析完成');
        displayBatchAnalysisResult(result.data);
        const householdCount = result.data.report_metadata?.区域信息?.户数 || 0;
        showMessage('success', `批量深度分析完成 (${householdCount}户)`);
    } else {
        throw new Error(result.message || '批量分析失败');
    }
}

// 更新分析进度
function updateAnalysisProgress(percent, text) {
    document.getElementById('progressFill').style.width = percent + '%';
    document.getElementById('progressPercent').textContent = percent + '%';
    document.getElementById('progressText').textContent = text;
    document.getElementById('analysisStatus').textContent = text;
}

// 显示单户分析结果
function displaySingleAnalysisResult(data) {
    console.log('displaySingleAnalysisResult called with data:', data);
    const resultsContainer = document.getElementById('analysisResults');
    console.log('resultsContainer found:', resultsContainer);

    if (!resultsContainer) {
        console.error('analysisResults container not found!');
        return;
    }

    try {
        console.log('Building simplified HTML template...');

        const basicInfo = data.household_basic_info || {};
        const comprehensive = data.comprehensive_assessment || {};
        const quality = data.quality_assessment || {};
        const anomaly = data.anomaly_detection || {};

        // 构建完整的HTML模板
        const html = `
            <div style="background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <div style="margin-bottom: 20px; padding-bottom: 15px; border-bottom: 2px solid #e9ecef;">
                    <h4 style="color: #333; margin-bottom: 10px;">
                        <i class="fas fa-user-check"></i> 单户深度分析结果
                    </h4>
                    <div style="display: flex; flex-wrap: wrap; gap: 20px; color: #666; font-size: 0.9rem;">
                        <span>户代码: ${basicInfo.户代码 || 'N/A'}</span>
                        <span>户主: ${basicInfo.户主姓名 || 'N/A'}</span>
                        <span>村居: ${basicInfo.村居名称 || 'N/A'}</span>
                        <span>生成时间: ${data.report_metadata?.报告生成时间 || 'N/A'}</span>
                    </div>
                </div>

                <div style="display: flex; flex-direction: column; gap: 20px;">
                    <!-- 基本信息 -->
                    <div style="background: #f8f9fa; border-radius: 6px; padding: 15px;">
                        <h5 style="color: #495057; margin-bottom: 15px;">
                            <i class="fas fa-info-circle"></i> 基本信息
                        </h5>
                        <table style="width: 100%; border-collapse: collapse;">
                            <tbody>
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">户代码</td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">${basicInfo.户代码 || 'N/A'}</td>
                                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">户主姓名</td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">${basicInfo.户主姓名 || 'N/A'}</td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">村居名称</td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">${basicInfo.村居名称 || 'N/A'}</td>
                                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">家庭人口</td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">${basicInfo.家庭人口 || 'N/A'}</td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">总收入</td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">${formatCurrency(basicInfo.总收入 || 0)}</td>
                                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">总支出</td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">${formatCurrency(basicInfo.总支出 || 0)}</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- 消费画像标签 -->
                    <div style="background: #f8f9fa; border-radius: 6px; padding: 15px;">
                        <h5 style="color: #495057; margin-bottom: 15px;">
                            <i class="fas fa-tags"></i> 消费画像标签
                        </h5>
                        <div style="overflow-x: auto;">
                            ${generateConsumptionProfileTable(data.consumption_profile)}
                        </div>
                    </div>

                    <!-- 记账质量评估 -->
                    <div style="background: #f8f9fa; border-radius: 6px; padding: 15px;">
                        <h5 style="color: #495057; margin-bottom: 15px;">
                            <i class="fas fa-star"></i> 记账质量评估
                        </h5>
                        <div style="overflow-x: auto;">
                            ${generateQualityAssessmentTable(quality)}
                        </div>
                    </div>

                    <!-- 综合评估 -->
                    <div style="background: #f8f9fa; border-radius: 6px; padding: 15px;">
                        <h5 style="color: #495057; margin-bottom: 15px;">
                            <i class="fas fa-chart-pie"></i> 综合评估
                        </h5>
                        <table style="width: 100%; border-collapse: collapse;">
                            <tbody>
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">综合等级</td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">
                                        <span style="padding: 2px 8px; border-radius: 4px; color: white; background: #28a745;">
                                            ${comprehensive.综合等级 || 'N/A'}
                                        </span>
                                    </td>
                                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">综合评分</td>
                                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>${comprehensive.综合评分 || 0}</strong></td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">评估描述</td>
                                    <td colspan="3" style="padding: 8px; border: 1px solid #ddd;">${comprehensive.评估描述 || '暂无描述'}</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- 异常检测结果 -->
                    <div style="background: #f8f9fa; border-radius: 6px; padding: 15px;">
                        <h5 style="color: #495057; margin-bottom: 15px;">
                            <i class="fas fa-exclamation-triangle"></i> 异常检测结果
                        </h5>
                        <div style="overflow-x: auto;">
                            ${generateAnomalyDetectionTable(anomaly)}
                        </div>
                    </div>
                </div>
            </div>
        `;

        console.log('Setting innerHTML...');
        resultsContainer.innerHTML = html;
        console.log('Setting display to block...');
        resultsContainer.style.display = 'block';
        console.log('displaySingleAnalysisResult completed successfully');

    } catch (error) {
        console.error('Error in displaySingleAnalysisResult:', error);
        resultsContainer.innerHTML = `
            <div style="background: #f8d7da; color: #721c24; padding: 15px; border-radius: 6px; border: 1px solid #f5c6cb;">
                <h5>显示分析结果时发生错误</h5>
                <p>错误信息: ${error.message}</p>
                <p>请检查浏览器控制台获取更多详细信息。</p>
            </div>
        `;
        resultsContainer.style.display = 'block';
    }
}

// 显示批量分析结果
function displayBatchAnalysisResult(data) {
    const resultsContainer = document.getElementById('analysisResults');
    const areaInfo = data.report_metadata?.区域信息;
    const stats = data.batch_statistics;
    const households = data.household_results || [];

    const html = `
        <div class="analysis-result-header">
            <h4><i class="fas fa-users-cog"></i> 批量深度分析结果</h4>
            <div class="result-meta">
                <span class="meta-item">区域: ${areaInfo?.乡镇 || ''}${areaInfo?.村庄 ? ' - ' + areaInfo.村庄 : ''}</span>
                <span class="meta-item">分析户数: ${areaInfo?.户数 || 0}</span>
                <span class="meta-item">有效户数: ${households.length}</span>
                <span class="meta-item">生成时间: ${data.report_metadata?.报告生成时间 || 'N/A'}</span>
            </div>
        </div>

        <div class="analysis-result-sections">
            <!-- 统计概览 -->
            <div class="result-section">
                <h5><i class="fas fa-chart-bar"></i> 统计概览</h5>
                <table class="analysis-result-table">
                    <thead>
                        <tr>
                            <th>统计项目</th>
                            <th>质量评估</th>
                            <th>异常检测</th>
                            <th>综合评估</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td class="label-cell">平均评分</td>
                            <td>${(stats?.质量评估统计?.平均质量评分 || 0).toFixed(2)}</td>
                            <td>${(stats?.异常检测统计?.平均异常评分 || 0).toFixed(2)}</td>
                            <td>${(stats?.综合评估统计?.平均综合评分 || 0).toFixed(2)}</td>
                        </tr>
                        <tr>
                            <td class="label-cell">最高评分</td>
                            <td>${stats?.质量评估统计?.最高质量评分 || 0}</td>
                            <td>${stats?.异常检测统计?.最高异常评分 || 0}</td>
                            <td>${stats?.综合评估统计?.最高综合评分 || 0}</td>
                        </tr>
                        <tr>
                            <td class="label-cell">最低评分</td>
                            <td>${stats?.质量评估统计?.最低质量评分 || 0}</td>
                            <td>${stats?.异常检测统计?.最低异常评分 || 0}</td>
                            <td>${stats?.综合评估统计?.最低综合评分 || 0}</td>
                        </tr>
                        <tr>
                            <td class="label-cell">总计数量</td>
                            <td>-</td>
                            <td>${stats?.异常检测统计?.总异常记录数 || 0} 条异常</td>
                            <td>-</td>
                        </tr>
                    </tbody>
                </table>
            </div>

            <!-- 质量等级分布 -->
            <div class="result-section">
                <h5><i class="fas fa-pie-chart"></i> 质量等级分布</h5>
                ${generateQualityDistributionTable(stats?.质量评估统计?.质量等级分布)}
            </div>

            <!-- 户分析结果列表 -->
            <div class="result-section">
                <h5><i class="fas fa-list"></i> 户分析结果列表</h5>
                <div class="table-container" style="max-height: 400px; overflow-y: auto;">
                    ${generateBatchHouseholdTable(households)}
                </div>
            </div>
        </div>
    `;

    resultsContainer.innerHTML = html;
    resultsContainer.style.display = 'block';
}

// 生成消费画像标签表格
function generateConsumptionProfileTable(profile) {
    if (!profile) {
        return '<p style="color: #6c757d; font-style: italic; text-align: center; padding: 20px;">暂无消费画像数据</p>';
    }

    let html = '<table style="width: 100%; border-collapse: collapse;"><tbody>';

    // 遍历所有标签类型
    for (const [tagType, tags] of Object.entries(profile)) {
        if (Array.isArray(tags) && tagType.endsWith('标签')) {
            html += `
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef; width: 120px;">${tagType}</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">
                        <div style="display: flex; flex-wrap: wrap; gap: 5px;">
                            ${tags.map(tag => `
                                <span style="
                                    background: #007bff;
                                    color: white;
                                    padding: 2px 8px;
                                    border-radius: 12px;
                                    font-size: 0.85rem;
                                    white-space: nowrap;
                                ">${tag}</span>
                            `).join('')}
                        </div>
                    </td>
                </tr>
            `;
        }
    }

    html += '</tbody></table>';
    return html;
}

// 生成异常检测结果表格
function generateAnomalyDetectionTable(anomalyData) {
    if (!anomalyData) {
        return '<p style="color: #6c757d; font-style: italic; text-align: center; padding: 20px;">暂无异常检测数据</p>';
    }

    const stats = anomalyData.异常统计 || {};
    const details = anomalyData.异常详情 || [];

    let html = `
        <table style="width: 100%; border-collapse: collapse;">
            <tbody>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">异常记录数</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">
                        <span style="color: #fd7e14; font-weight: 600;">${stats.异常记录数 || 0}</span>
                    </td>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">异常评分</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">
                        <span style="color: #fd7e14; font-weight: 600;">${stats.异常评分 || 0}</span>
                    </td>
                </tr>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">异常类型分布</td>
                    <td colspan="3" style="padding: 8px; border: 1px solid #ddd;">
                        <div style="display: flex; flex-wrap: wrap; gap: 5px;">
                            ${Object.entries(stats.异常类型分布 || {}).map(([type, count]) =>
                                `<span style="
                                    background: #dc3545;
                                    color: white;
                                    padding: 2px 8px;
                                    border-radius: 12px;
                                    font-size: 0.85rem;
                                    white-space: nowrap;
                                ">${type}: ${count}</span>`
                            ).join('')}
                        </div>
                    </td>
                </tr>
            </tbody>
        </table>
    `;

    // 如果有异常详情，显示异常记录表格
    if (details.length > 0) {
        html += `
            <h6 style="margin-top: 15px; color: #495057; font-size: 1rem;">
                <i class="fas fa-list"></i> 异常记录详情
            </h6>
            <div style="max-height: 300px; overflow-y: auto; border: 1px solid #ddd; border-radius: 4px;">
                <table style="width: 100%; border-collapse: collapse;">
                    <thead>
                        <tr style="background: #f8f9fa;">
                            <th style="padding: 8px; border: 1px solid #ddd; font-weight: bold; text-align: left;">日期</th>
                            <th style="padding: 8px; border: 1px solid #ddd; font-weight: bold; text-align: left;">类型</th>
                            <th style="padding: 8px; border: 1px solid #ddd; font-weight: bold; text-align: left;">金额</th>
                            <th style="padding: 8px; border: 1px solid #ddd; font-weight: bold; text-align: left;">异常原因</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${details.slice(0, 10).map(item => `
                            <tr>
                                <td style="padding: 8px; border: 1px solid #ddd;">${formatChineseDate(item.日期)}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">${item.类型 || 'N/A'}</td>
                                <td style="padding: 8px; border: 1px solid #ddd; font-weight: 600; color: #dc3545;">${formatCurrency(item.金额)}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">${item.异常原因 || 'N/A'}</td>
                            </tr>
                        `).join('')}
                        ${details.length > 10 ? `
                            <tr>
                                <td colspan="4" style="padding: 8px; border: 1px solid #ddd; text-align: center; color: #6c757d; font-style: italic;">
                                    ... 还有 ${details.length - 10} 条记录
                                </td>
                            </tr>
                        ` : ''}
                    </tbody>
                </table>
            </div>
        `;
    }

    return html;
}

// 生成记账质量评估表格
function generateQualityAssessmentTable(qualityData) {
    if (!qualityData) {
        return '<p style="color: #6c757d; font-style: italic; text-align: center; padding: 20px;">暂无质量评估数据</p>';
    }

    const scores = qualityData.各项评分 || {};

    // 获取质量等级对应的颜色
    const getQualityColor = (level) => {
        const colorMap = {
            '优秀': '#28a745',
            '良好': '#17a2b8',
            '一般': '#ffc107',
            '较差': '#fd7e14',
            '很差': '#dc3545'
        };
        return colorMap[level] || '#6c757d';
    };

    let html = `
        <table style="width: 100%; border-collapse: collapse;">
            <tbody>
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">质量等级</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">
                        <span style="
                            padding: 2px 8px;
                            border-radius: 4px;
                            color: white;
                            background: ${getQualityColor(qualityData.质量评估)};
                            font-weight: 600;
                        ">${qualityData.质量评估 || 'N/A'}</span>
                    </td>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold; background: #e9ecef;">总评分</td>
                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>${qualityData.总评分 || 0}</strong></td>
                </tr>
            </tbody>
        </table>
    `;

    // 如果有详细评分信息，显示质量指标详情
    if (Object.keys(scores).length > 0) {
        html += `
            <h6 style="margin-top: 15px; color: #495057; font-size: 1rem;">
                <i class="fas fa-chart-bar"></i> 质量指标详情
            </h6>
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #f8f9fa;">
                        <th style="padding: 8px; border: 1px solid #ddd; font-weight: bold; text-align: left;">指标名称</th>
                        <th style="padding: 8px; border: 1px solid #ddd; font-weight: bold; text-align: left;">得分</th>
                        <th style="padding: 8px; border: 1px solid #ddd; font-weight: bold; text-align: left;">权重</th>
                        <th style="padding: 8px; border: 1px solid #ddd; font-weight: bold; text-align: left;">加权得分</th>
                    </tr>
                </thead>
                <tbody>
                    ${Object.entries(scores).map(([indicator, score]) => {
                        const weight = qualityData.评分权重?.[indicator] || 0;
                        const weightedScore = (score * weight).toFixed(2);
                        return `
                            <tr>
                                <td style="padding: 8px; border: 1px solid #ddd;">${indicator}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;">
                                    <span style="color: #007bff; font-weight: 600;">${score || 0}</span>
                                </td>
                                <td style="padding: 8px; border: 1px solid #ddd;">${weight}</td>
                                <td style="padding: 8px; border: 1px solid #ddd;"><strong>${weightedScore}</strong></td>
                            </tr>
                        `;
                    }).join('')}
                </tbody>
            </table>
        `;
    }

    return html;
}

// 生成质量等级分布表格
function generateQualityDistributionTable(distribution) {
    if (!distribution) {
        return '<p class="no-data">暂无分布数据</p>';
    }

    const total = Object.values(distribution).reduce((sum, count) => sum + count, 0);

    let html = '<table class="analysis-result-table"><tbody>';

    for (const [level, count] of Object.entries(distribution)) {
        const percentage = total > 0 ? ((count / total) * 100).toFixed(1) : 0;
        html += `
            <tr>
                <td class="label-cell">
                    <span class="quality-badge quality-${getQualityClass(level)}">${level}</span>
                </td>
                <td>${count} 户</td>
                <td>${percentage}%</td>
                <td>
                    <div class="progress-bar" style="width: 200px; height: 6px;">
                        <div class="progress-fill" style="width: ${percentage}%"></div>
                    </div>
                </td>
            </tr>
        `;
    }

    html += '</tbody></table>';
    return html;
}

// 生成批量户分析结果表格
function generateBatchHouseholdTable(households) {
    if (!households || households.length === 0) {
        return '<p class="no-data">暂无户分析数据</p>';
    }

    let html = `
        <table class="analysis-result-table">
            <thead>
                <tr>
                    <th>序号</th>
                    <th>户代码</th>
                    <th>户主姓名</th>
                    <th>村居名称</th>
                    <th>质量等级</th>
                    <th>质量评分</th>
                    <th>异常记录数</th>
                    <th>综合评分</th>
                </tr>
            </thead>
            <tbody>
    `;

    households.forEach((household, index) => {
        const basic = household.household_basic_info || {};
        const quality = household.quality_assessment || {};
        const anomaly = household.anomaly_detection?.异常统计 || {};
        const comprehensive = household.comprehensive_assessment || {};

        html += `
            <tr>
                <td>${index + 1}</td>
                <td><code>${basic.户代码 || 'N/A'}</code></td>
                <td>${basic.户主姓名 || 'N/A'}</td>
                <td>${basic.村居名称 || 'N/A'}</td>
                <td><span class="quality-badge quality-${getQualityClass(quality.质量评估)}">${quality.质量评估 || 'N/A'}</span></td>
                <td><strong>${quality.总评分 || 0}</strong></td>
                <td><span class="anomaly-count">${anomaly.异常记录数 || 0}</span></td>
                <td><strong>${comprehensive.综合评分 || 0}</strong></td>
            </tr>
        `;
    });

    html += '</tbody></table>';
    return html;
}

// 获取质量等级对应的CSS类
function getQualityClass(level) {
    const classMap = {
        '优秀': 'excellent',
        '良好': 'good',
        '一般': 'average',
        '较差': 'poor',
        '很差': 'very-poor'
    };
    return classMap[level] || 'unknown';
}

// 初始化深度分析事件监听器
function initializeAnalysisEventListeners() {
    // 开始分析按钮
    document.getElementById('startAnalysis').addEventListener('click', startDeepAnalysis);

    // 停止分析按钮
    document.getElementById('stopAnalysis').addEventListener('click', stopAnalysis);

    // 监听筛选条件变化
    const filterElements = ['filterTown', 'filterVillage', 'filterHousehold', 'startDate', 'endDate'];
    filterElements.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.addEventListener('change', () => {
                const activeTab = document.querySelector('.tab-item.active')?.dataset.tab;
                if (activeTab === 'analysis') {
                    updateAnalysisScope();
                }
            });
        }
    });
}
