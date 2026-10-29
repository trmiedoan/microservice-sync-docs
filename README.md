# Roadmap thực hành: Backend Data Synchronization POC

## Mục tiêu cuối

Tự xây dựng một POC bằng ASP.NET Core / EF Core, SQL Server và RabbitMQ
để đồng bộ dữ liệu từ một database nguồn đến một hoặc nhiều database
đích. POC có Transactional Outbox, retry có giới hạn, idempotent
consumer, log/trạng thái, và kiểm thử các tình huống lỗi.

**Cách học:** đọc yêu cầu → tìm hiểu đúng các concept được chỉ định → tự
code → chạy checkwork → ghi lại điều đã hiểu → chỉ sang phase tiếp theo
khi qua Review Gate. Không copy nguyên lời giải từ AI. Có thể dùng AI để
giải thích lỗi hoặc review sau khi đã thử.

## Phạm vi và giả định

-   Dùng **RabbitMQ** cho phiên bản đầu tiên; không học RabbitMQ và
    Kafka đồng thời.
-   Dùng **C# / ASP.NET Core / EF Core / SQL Server** vì đây là stack
    người học đã dùng.
-   Dùng Docker Compose nếu có thể để chạy RabbitMQ và các
    database/service phụ trợ; nếu chưa quen Docker, có thể chạy RabbitMQ
    theo hướng dẫn chính thức rồi học Docker sau.
-   Bắt đầu với một loại dữ liệu đơn giản, ví dụ `Product` hoặc
    `Student`. Không cần frontend.
-   Mục tiêu là **eventual consistency**: nếu hệ thống phục hồi và không
    còn lỗi vĩnh viễn, các đích hợp lệ cuối cùng sẽ nhận được trạng thái
    đúng.
-   "Không mất dữ liệu" cần được diễn giải thành các cam kết cụ thể.
    RabbitMQ không thể tự mình bảo đảm end-to-end exactly-once trên
    nhiều database.
-   Saga là concept riêng để phối hợp quy trình nghiệp vụ nhiều bước;
    không bắt buộc cho luồng đồng bộ dữ liệu đơn giản. Phase 6 có phần
    học/mini-exercise Saga, nhưng chỉ tích hợp vào sản phẩm cuối nếu
    mentor xác nhận có use case phù hợp.

## Sơ đồ 6 phase

1.  **Nền tảng .NET cho worker** --- DI, async/await, transaction,
    logging, background worker.
2.  **RabbitMQ cơ bản** --- producer, consumer, queue, exchange,
    routing, acknowledgement.
3.  **Delivery reliability** --- publisher confirms, retry, backoff,
    dead-letter queue, phục hồi khi broker/consumer lỗi.
4.  **Transactional Outbox** --- lưu thay đổi nghiệp vụ và event trong
    cùng transaction; worker phát outbox.
5.  **Idempotency và fan-out nhiều database** --- một nguồn tới nhiều
    đích, deduplication, trạng thái theo từng đích, kiểm tra thứ tự.
6.  **Failure testing, quan sát hệ thống và kiến trúc tổng hợp** ---
    fault injection, reconciliation, tài liệu vận hành; tìm hiểu Saga và
    quyết định có cần áp dụng hay không.

## Cách sử dụng bộ đề

Mỗi file phase gồm: - Prerequisites: cần biết gì / chưa cần biết gì. -
Kiến thức cần học theo thứ tự và tài liệu. - Đề bài tiny project có đầu
vào, đầu ra và ràng buộc. - Acceptance criteria. - Checkwork gồm câu
hỏi, bài code bổ sung và test lỗi. - Pitfalls cần tự phát hiện. - Review
Gate để quyết định đã sẵn sàng sang phase tiếp theo chưa.

### Quy tắc tự đánh giá

Chỉ đánh dấu một checkwork là đạt khi bạn: 1. Có thể chạy lại được từ
trạng thái sạch theo README. 2. Có thể giải thích bằng lời của mình,
không chỉ đọc code. 3. Có log/test/output làm bằng chứng. 4. Biết giới
hạn nào chưa được xử lý.

## Bối cảnh nghiệp vụ xuyên suốt

Bộ bài 6 phase giả định bạn đang xây backend cho **ShopMini**, một cửa hàng bán đồ công nghệ nhỏ. Nguồn dữ liệu danh mục sản phẩm là Product Catalog. Hai hệ thống khác cần bản sao sản phẩm:
- **Storefront DB:** phục vụ website bán hàng.
- **Reporting DB:** phục vụ báo cáo nội bộ.

Khi nhân viên tạo/sửa/xóa sản phẩm ở nguồn, thay đổi cần được phát thành event và đồng bộ tới hai đích. Các đích có thể tạm thời không hoạt động; message có thể được giao lại; một đích có thể cập nhật trước đích còn lại. POC cần làm rõ trạng thái, retry, deduplication và cách kiểm tra dữ liệu cuối cùng hội tụ.

Các phase đầu cố ý dùng bài tập nhỏ hơn để học từng mảnh:
- Phase 1: worker xử lý tác vụ nội bộ của ShopMini, chưa có broker.
- Phase 2–3: luồng thông báo xác nhận đơn hàng để học producer/consumer và delivery reliability.
- Phase 4–6: quay về bài toán Product Catalog → Storefront DB + Reporting DB, vì đây là use case đồng bộ dữ liệu cuối cùng.

Đây là bối cảnh giả định để tạo mục tiêu cụ thể cho bài tập, không phải khẳng định về hệ thống thật của mentor. Nếu mentor cung cấp nghiệp vụ thực tế khác, hãy giữ nguyên các concept đã học nhưng thay entity, quy tắc và tiêu chí kiểm thử cho khớp.


## Tài liệu chính

-   .NET dependency injection:
    https://learn.microsoft.com/en-us/dotnet/core/extensions/dependency-injection
-   Hosted services:
    https://learn.microsoft.com/en-us/dotnet/core/extensions/workers
-   EF Core transactions:
    https://learn.microsoft.com/en-us/ef/core/saving/transactions
-   EF Core DbContext lifetime:
    https://learn.microsoft.com/en-us/ef/core/dbcontext-configuration/
-   RabbitMQ tutorials: https://www.rabbitmq.com/tutorials
-   RabbitMQ publisher confirms: https://www.rabbitmq.com/docs/confirms
-   RabbitMQ consumers / acknowledgements:
    https://www.rabbitmq.com/docs/consumers
-   RabbitMQ dead lettering: https://www.rabbitmq.com/docs/dlx
-   Transactional Outbox:
    https://microservices.io/patterns/data/transactional-outbox.html
-   Idempotent Consumer:
    https://microservices.io/patterns/communication-style/idempotent-consumer.html
-   Saga: https://microservices.io/patterns/data/saga.html
