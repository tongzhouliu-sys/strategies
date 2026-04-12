// disclaimer.js — 全局免责声明控制器（零依赖，延迟执行）
document.addEventListener('DOMContentLoaded', () => {
  // 1. 智能定位容器（优先插入 main 末尾，兜底插入 body）
  let box = document.getElementById('global-disclaimer');
  if (!box) {
    box = document.createElement('div');
    box.id = 'global-disclaimer';
    box.className = 'no-print';
    const target = document.querySelector('.main-content') || document.body;
    target.appendChild(box);
  }

  // 2. 注入内容（后期仅需修改此模板）
  const now = new Date();
  box.innerHTML = `
    <div style="margin-top: 3rem; padding-top: 2rem; border-top: 1px solid var(--c-border, #e2e8f0); color: #64748b; font-size: 0.85rem; line-height: 1.6;">
      <h4 style="font-size: 1rem; color: #0f172a; margin: 0 0 0.5rem;">⚠️ 免责声明</h4>
      <p style="margin: 0 0 0.5rem;">本页面策略内容仅供学习交流与模拟参考，不构成任何投资建议或税务/法律意见。期权交易具有高风险，可能导致本金全部损失。请根据自身风险承受能力独立决策，或咨询持牌专业顾问。</p>
      <p style="margin: 0;">© ${now.getFullYear()} 策略研究组 | 最后更新：<time datetime="${now.toISOString().split('T')[0]}">${now.toLocaleDateString()}</time></p>
    </div>
  `;
});
