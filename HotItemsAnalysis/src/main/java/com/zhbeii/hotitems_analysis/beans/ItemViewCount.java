package com.zhbeii.hotitems_analysis.beans;/*
@author Zhbeii
@date 2022/1/3 - 20:34
*/


/**
 * 中间窗口的聚合包装：
 * 1. 当前的窗口id
 * 2. 属于哪一个窗口
 * 3. 最后的count值是多少
 */
public class ItemViewCount {

    private Long itemId;
    private Long windowEnd;
    private Long count;

    public ItemViewCount() {

    }

    public ItemViewCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
