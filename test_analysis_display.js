// 测试深度分析显示功能的JavaScript代码

// 模拟数据
const mockAnalysisData = {
    household_basic_info: {
        户代码: '321283001002012',
        户主姓名: '测试户主',
        村居名称: '测试村',
        家庭人口: 3,
        总收入: 50000,
        总支出: 30000
    },
    consumption_profile: {
        消费水平型标签: ['高消费户'],
        消费偏好型标签: ['爱宠家庭', '健康生活']
    },
    quality_assessment: {
        质量评估: '优秀',
        总评分: 95,
        各项评分: {
            数据完整性: 100,
            记账频率: 95,
            数据一致性: 90
        },
        评分权重: {
            数据完整性: 0.4,
            记账频率: 0.3,
            数据一致性: 0.3
        }
    },
    comprehensive_assessment: {
        综合等级: '优秀',
        综合评分: 95,
        评估描述: '记账质量优秀，数据可靠性高，建议继续保持良好的记账习惯。'
    },
    anomaly_detection: {
        异常统计: {
            异常记录数: 5,
            异常评分: 2.5,
            异常类型分布: {
                '金额异常': 3,
                '频率异常': 2
            }
        },
        异常详情: [
            {
                日期: '2025-08-15',
                类型: '食品',
                金额: 5000,
                异常原因: '单次消费金额过高'
            },
            {
                日期: '2025-08-10',
                类型: '交通',
                金额: 2000,
                异常原因: '消费频率异常'
            }
        ]
    },
    report_metadata: {
        报告生成时间: '2025-08-22 15:30:00'
    }
};

// 测试函数
function testAnalysisDisplay() {
    console.log('开始测试深度分析显示功能...');
    
    // 检查必要的函数是否存在
    const requiredFunctions = [
        'formatCurrency',
        'getQualityClass',
        'generateConsumptionProfileTable',
        'generateQualityAssessmentTable',
        'generateAnomalyDetectionTable',
        'displaySingleAnalysisResult'
    ];
    
    const missingFunctions = [];
    requiredFunctions.forEach(funcName => {
        if (typeof window[funcName] !== 'function') {
            missingFunctions.push(funcName);
        }
    });
    
    if (missingFunctions.length > 0) {
        console.error('缺少以下函数:', missingFunctions);
        return false;
    }
    
    // 检查DOM元素是否存在
    const analysisResults = document.getElementById('analysisResults');
    if (!analysisResults) {
        console.error('找不到analysisResults元素');
        return false;
    }
    
    // 测试显示功能
    try {
        displaySingleAnalysisResult(mockAnalysisData);
        console.log('✅ 深度分析显示功能测试成功');
        return true;
    } catch (error) {
        console.error('❌ 深度分析显示功能测试失败:', error);
        return false;
    }
}

// 在页面加载完成后执行测试
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', testAnalysisDisplay);
} else {
    testAnalysisDisplay();
}
