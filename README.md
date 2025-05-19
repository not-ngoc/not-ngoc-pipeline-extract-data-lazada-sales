# Phân tích dữ liệu Lazada Sales

## Giới thiệu
Dự án này thực hiện luồng ETL để chuẩn bị data cho phân tích dữ liệu bán hàng từ Lazada, sử dụng 2 nguồn dữ liệu chính là bảng Sales và Product để tạo báo cáo tổng quan trên Power BI, từ đó đưa ra các insights kinh doanh.

## Mục tiêu: Tạo data nhằm
- Phân tích xu hướng bán hàng theo thời gian
- Phân tích hiệu suất sản phẩm (SKU)
- Tạo dashboard trực quan trên Power BI
- Đưa ra các insights giúp cải thiện hiệu quả kinh doanh

## Cấu trúc dữ liệu input
### 1. Bảng Sales
- Thông tin về giao dịch bán hàng
- Các trường dữ liệu chính: Date, SKU, Qty

### 2. Bảng Product
- Thông tin chi tiết về sản phẩm
- Liên kết với bảng Sales thông qua trường SKU


