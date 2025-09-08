/**
 * 前端通用工具类
 * 减少JavaScript代码重复
 */

class UIHelper {
    /**
     * 显示加载状态
     * @param {string} message - 加载消息
     */
    static showLoading(message = '正在加载...') {
        const overlay = document.getElementById('loadingOverlay');
        const text = document.getElementById('loadingText');
        if (overlay && text) {
            text.textContent = message;
            overlay.style.display = 'flex';
        }
    }

    /**
     * 隐藏加载状态
     */
    static hideLoading() {
        const overlay = document.getElementById('loadingOverlay');
        if (overlay) {
            overlay.style.display = 'none';
        }
    }

    /**
     * 显示消息
     * @param {string} type - 消息类型 (success, error, warning, info)
     * @param {string} message - 消息内容
     * @param {number} duration - 显示时长（毫秒）
     */
    static showMessage(type, message, duration = 5000) {
        const container = document.getElementById('messageContainer');
        if (!container) return;

        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${type}`;
        
        const iconMap = {
            'success': 'check-circle',
            'error': 'exclamation-circle',
            'warning': 'exclamation-triangle',
            'info': 'info-circle'
        };

        messageDiv.innerHTML = `
            <i class="fas fa-${iconMap[type] || 'info-circle'}"></i>
            <span>${message}</span>
            <button class="message-close" onclick="this.parentElement.remove()">×</button>
        `;

        container.appendChild(messageDiv);

        // 自动移除消息
        setTimeout(() => {
            if (messageDiv.parentElement) {
                messageDiv.remove();
            }
        }, duration);
    }

    /**
     * 显示成功消息
     * @param {string} message - 消息内容
     */
    static showSuccess(message) {
        this.showMessage('success', message);
    }

    /**
     * 显示错误消息
     * @param {string} message - 消息内容
     */
    static showError(message) {
        this.showMessage('error', message);
    }

    /**
     * 显示警告消息
     * @param {string} message - 消息内容
     */
    static showWarning(message) {
        this.showMessage('warning', message);
    }
}

class DataFormatter {
    /**
     * 格式化数字
     * @param {number} num - 数字
     * @returns {string} 格式化后的数字字符串
     */
    static formatNumber(num) {
        if (num === null || num === undefined) return '0';
        return num.toLocaleString();
    }

    /**
     * 格式化货币
     * @param {number} amount - 金额
     * @returns {string} 格式化后的货币字符串
     */
    static formatCurrency(amount) {
        if (amount === null || amount === undefined) return '¥0.00';
        return '¥' + parseFloat(amount).toLocaleString('zh-CN', {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });
    }

    /**
     * 格式化百分比
     * @param {number} value - 数值
     * @param {number} total - 总数
     * @param {number} decimals - 小数位数
     * @returns {string} 百分比字符串
     */
    static formatPercentage(value, total, decimals = 1) {
        if (!total || total === 0) return '0%';
        return ((value / total) * 100).toFixed(decimals) + '%';
    }
}

class APIClient {
    /**
     * 发送GET请求
     * @param {string} url - 请求URL
     * @param {Object} params - 查询参数
     * @returns {Promise<Object>} 响应数据
     */
    static async get(url, params = {}) {
        const queryString = new URLSearchParams(params).toString();
        const fullUrl = queryString ? `${url}?${queryString}` : url;
        
        const response = await fetch(fullUrl);
        const data = await response.json();
        
        if (!data.success) {
            throw new Error(data.message || '请求失败');
        }
        
        return data.data;
    }

    /**
     * 发送POST请求
     * @param {string} url - 请求URL
     * @param {Object} data - 请求数据
     * @returns {Promise<Object>} 响应数据
     */
    static async post(url, data = {}) {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        });
        
        const result = await response.json();
        
        if (!result.success) {
            throw new Error(result.message || '请求失败');
        }
        
        return result.data;
    }

    /**
     * 上传文件
     * @param {string} url - 上传URL
     * @param {FormData} formData - 表单数据
     * @returns {Promise<Object>} 响应数据
     */
    static async uploadFile(url, formData) {
        const response = await fetch(url, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (!result.success) {
            throw new Error(result.message || '上传失败');
        }
        
        return result.data;
    }
}

class TableHelper {
    /**
     * 更新表格数据
     * @param {string} tableId - 表格ID
     * @param {Array} data - 数据数组
     * @param {Function} rowRenderer - 行渲染函数
     */
    static updateTable(tableId, data, rowRenderer) {
        const tbody = document.querySelector(`#${tableId} tbody`);
        if (!tbody) return;

        tbody.innerHTML = '';

        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.innerHTML = rowRenderer(row);
            tbody.appendChild(tr);
        });
    }

    /**
     * 渲染分户统计表格行
     * @param {Object} row - 数据行
     * @returns {string} HTML字符串
     */
    static renderHouseholdRow(row) {
        const codingRate = row.记账笔数 > 0 ?
            DataFormatter.formatPercentage(row.已编码笔数, row.记账笔数) : '0%';

        return `
            <td>${row.户代码}</td>
            <td>${row.户主姓名}</td>
            <td>${row.年份}</td>
            <td>${row.月份}</td>
            <td>${row.记账笔数}</td>
            <td>${row.收入笔数}</td>
            <td>${row.支出笔数}</td>
            <td>${DataFormatter.formatCurrency(row.收入总额)}</td>
            <td>${DataFormatter.formatCurrency(row.支出总额)}</td>
            <td>${codingRate}</td>
        `;
    }

    /**
     * 渲染分乡镇统计表格行
     * @param {Object} row - 数据行
     * @returns {string} HTML字符串
     */
    static renderTownRow(row) {
        const codingRate = row.记账笔数 > 0 ?
            DataFormatter.formatPercentage(row.已编码笔数, row.记账笔数) : '0%';

        return `
            <td>${row.乡镇名称}</td>
            <td>${row.户数}</td>
            <td>${row.记账笔数}</td>
            <td>${row.收入笔数}</td>
            <td>${row.支出笔数}</td>
            <td>${DataFormatter.formatCurrency(row.收入总额)}</td>
            <td>${DataFormatter.formatCurrency(row.支出总额)}</td>
            <td>${codingRate}</td>
        `;
    }

    /**
     * 渲染消费结构表格行
     * @param {Object} row - 数据行
     * @returns {string} HTML字符串
     */
    static renderConsumptionRow(row) {
        return `
            <td>${row.消费类别 || '未知'}</td>
            <td>${row.编码}</td>
            <td>${row.记账笔数}</td>
            <td>${DataFormatter.formatCurrency(row.总金额)}</td>
            <td>${DataFormatter.formatCurrency(row.平均金额)}</td>
            <td>${row.涉及户数}</td>
        `;
    }
}

class ChartHelper {
    /**
     * 创建图表的通用配置
     * @param {string} type - 图表类型
     * @param {Object} data - 图表数据
     * @param {Object} options - 自定义选项
     * @returns {Object} 图表配置对象
     */
    static createChartConfig(type, data, options = {}) {
        const defaultOptions = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            }
        };

        return {
            type: type,
            data: data,
            options: { ...defaultOptions, ...options }
        };
    }

    /**
     * 安全地销毁图表
     * @param {Chart} chart - Chart.js实例
     */
    static destroyChart(chart) {
        if (chart) {
            chart.destroy();
        }
    }

    /**
     * 更新或创建图表
     * @param {string} canvasId - 画布ID
     * @param {Chart} existingChart - 现有图表实例
     * @param {Object} config - 图表配置
     * @returns {Chart} 新的图表实例
     */
    static updateOrCreateChart(canvasId, existingChart, config) {
        this.destroyChart(existingChart);
        const ctx = document.getElementById(canvasId).getContext('2d');
        return new Chart(ctx, config);
    }
}

class FormHelper {
    /**
     * 获取表单数据
     * @param {string} formId - 表单ID
     * @returns {Object} 表单数据对象
     */
    static getFormData(formId) {
        const form = document.getElementById(formId);
        const formData = new FormData(form);
        const data = {};
        
        for (let [key, value] of formData.entries()) {
            data[key] = value;
        }
        
        return data;
    }

    /**
     * 重置表单
     * @param {string} formId - 表单ID
     */
    static resetForm(formId) {
        const form = document.getElementById(formId);
        if (form) {
            form.reset();
        }
    }

    /**
     * 填充下拉框选项
     * @param {string} selectId - 下拉框ID
     * @param {Array} options - 选项数组
     * @param {string} defaultText - 默认文本
     */
    static populateSelect(selectId, options, defaultText = '') {
        const select = document.getElementById(selectId);
        if (!select) return;

        select.innerHTML = '';
        
        if (defaultText) {
            select.add(new Option(defaultText, ''));
        }

        options.forEach(option => {
            if (typeof option === 'string') {
                select.add(new Option(option, option));
            } else if (option.text && option.value !== undefined) {
                select.add(new Option(option.text, option.value));
            }
        });
    }
}

// 导出工具类（如果使用ES6模块）
// export { UIHelper, DataFormatter, APIClient, TableHelper, ChartHelper, FormHelper };