# Phase 4 --- Transactional Outbox

**Mục tiêu:** giải quyết lỗi kinh điển: database đã commit thay đổi
nghiệp vụ nhưng ứng dụng chết trước khi publish message.

**Ước lượng:** 4--6 buổi; đây là một trong những phase quan trọng nhất.

## 0. Prerequisites

### Cần biết

-   EF Core và transaction trong một database.
-   Producer/consumer RabbitMQ.
-   Publisher confirms ở mức cơ bản.
-   Logging và background worker.

### Chưa cần biết

-   Saga.
-   Multi-region broker.
-   Exactly-once end-to-end.
-   Distributed transaction giữa nhiều database.

## 1. Kiến thức cần học --- theo thứ tự

1.  Dual-write problem: ghi database và publish message là hai thao tác
    riêng.
2.  Local database transaction và commit/rollback.
3.  Outbox table và outbox event.
4.  Worker polling outbox và publish message.
5.  Trạng thái `Pending`, `InProgress` (nếu dùng), `Published`/`Failed`,
    attempt count, timestamps.
6.  Vì sao publisher có thể publish trùng khi broker đã nhận nhưng ứng
    dụng chưa kịp đánh dấu outbox là published.
7.  Các vấn đề vận hành: xử lý nhiều worker, claim/lease, retry và dọn
    dữ liệu cũ ở mức phù hợp POC.

Tài liệu: -
https://microservices.io/patterns/data/transactional-outbox.html -
https://learn.microsoft.com/en-us/ef/core/saving/transactions -
https://www.rabbitmq.com/docs/confirms

## 2. Đề bài tiny project --- Product Change Publisher

Tạo một API/service quản lý `Product` ở database nguồn và phát event khi
product được tạo hoặc sửa.

ShopMini có một **Product Catalog API** dùng để nhân viên tạo hoặc cập nhật sản phẩm trong danh mục chính. Sau này, website bán hàng và hệ thống báo cáo cần nhận các thay đổi này. Nếu API ghi Product vào SQL Server thành công nhưng tiến trình chết trước khi gửi event, các hệ thống nhận sẽ không biết sản phẩm đã thay đổi.

Phase này xây phần nguồn của quy trình: lưu thay đổi sản phẩm và ghi lại ý định phát event một cách nguyên tử. Chưa cần xây database đích.

### Quy tắc nghiệp vụ

- Nhân viên tạo sản phẩm với `Name` và `Price` hợp lệ hoặc cập nhật giá/tên sản phẩm hiện có.
- Mỗi lần tạo hoặc cập nhật thành công, hệ thống phải có một event tương ứng trong `OutboxMessages`.
- Product và outbox event phải commit hoặc rollback cùng nhau.
- Outbox worker chịu trách nhiệm publish event sau đó; API không phụ thuộc RabbitMQ phải sẵn sàng ngay trong request.
- Nếu RabbitMQ tạm tắt, event phải còn trong database nguồn để worker có thể thử lại khi broker hoạt động.
- Event phải mang snapshot dữ liệu cần thiết, `EventId`, `EventType`, `OccurredAtUtc` và `SchemaVersion`.

### Kịch bản demo bắt buộc

1. Tạo `Product` `PRD-1001`, tên “USB-C Hub”, giá `25.00` → cả Product và outbox event được lưu.
2. Tắt worker, cập nhật giá thành `29.00` → API vẫn lưu Product và event pending.
3. Bật worker → event pending được publish.
4. Tạo một lỗi có chủ đích trong transaction → chứng minh không tồn tại trường hợp chỉ Product hoặc chỉ outbox event được commit.

Đơn vị tiền tệ có thể quy ước là USD hoặc VND trong README; chọn một và dùng nhất quán. Giá chỉ để làm dữ liệu demo, không cần tích hợp thanh toán.
"""

### Bảng nghiệp vụ

`Products`: - `Id` - `Name` - `Price` - `UpdatedAtUtc`

### Bảng outbox gợi ý

`OutboxMessages`: - `EventId` (UUID, PK) - `EventType` - `Payload` -
`OccurredAtUtc` - `Status` - `AttemptCount` - `NextAttemptAtUtc`
nullable - `PublishedAtUtc` nullable - `LastError` nullable

Có thể thêm cột claim/lease nếu cần nhiều worker; giải thích quyết định
nếu chưa triển khai.

### Yêu cầu

-   Khi tạo/sửa product, lưu thay đổi `Products` và bản ghi
    `OutboxMessages` trong **cùng một database transaction**.
-   Nếu transaction thất bại, cả hai thay đổi đều không được commit.
-   Background worker đọc outbox còn pending và publish lên RabbitMQ.
-   Chỉ đánh dấu published khi có bằng chứng xác nhận theo thiết kế đã
    chọn.
-   Nếu publish thất bại, lưu trạng thái/lỗi và retry theo chiến lược từ
    Phase 3.
-   Event có `EventId`, `EventType`, `OccurredAtUtc`, `SchemaVersion` và
    payload.
-   Không publish trực tiếp từ controller rồi mới lưu database như đường
    đi chính.

### Đầu vào

API tạo/sửa Product.

### Đầu ra

Database nguồn lưu Product và Outbox event nhất quán; event được phát ra
RabbitMQ sau đó, kể cả khi worker tạm thời không hoạt động lúc request
tạo Product.

### Ràng buộc

-   Một database nguồn và một queue.
-   Chưa yêu cầu nhiều database đích.
-   Không tuyên bố publish chỉ một lần; phải giải thích duplicate
    window.
-   Không đánh dấu outbox published trước khi publish/confirmation theo
    thiết kế.

## 3. Acceptance criteria

-   [ ] Product và outbox event commit cùng transaction.
-   [ ] Có test chứng minh rollback không để lại một nửa dữ liệu.
-   [ ] Dừng worker, tạo Product, rồi bật worker: event vẫn được phát.
-   [ ] Có retry và trạng thái lỗi của outbox.
-   [ ] Có thể tìm một event bằng `EventId` xuyên suốt DB và log.
-   [ ] README có sơ đồ transaction và worker.
-   [ ] Nêu rõ vì sao duplicate vẫn có thể xảy ra.

## 4. Checkwork

### Câu hỏi giải thích

1.  Vì sao `SaveChangesAsync()` rồi publish message không đủ an toàn?
2.  Outbox khắc phục dual-write problem bằng cách nào?
3.  Vì sao không thể gộp SQL transaction và RabbitMQ publish thành một
    transaction ACID đơn giản?
4.  Kịch bản nào khiến event đã được publish nhưng outbox vẫn còn
    `Pending`?
5.  Nếu hai worker cùng chọn một outbox row thì điều gì có thể xảy ra?
6.  Khi nào nên dùng polling, và polling có chi phí gì?

### Bài code bổ sung

-   Thêm event `ProductDeleted`.
-   Thêm `NextAttemptAtUtc` và retry backoff.
-   Viết một script query outbox theo trạng thái.
-   Tạo test cho transaction rollback.

### Tình huống lỗi

-   Database commit thành công nhưng worker tắt.
-   RabbitMQ tắt trong lúc worker publish.
-   Broker nhận message nhưng kết nối đứt trước khi worker nhận confirm.
-   Worker chết sau publish nhưng trước khi đánh dấu `Published`.
-   Có hai worker cùng chạy.

## 5. Pitfalls --- tự kiểm tra

-   [ ] Product và Outbox có thực sự ở cùng database transaction không?
-   [ ] Có đánh dấu published trước khi broker xác nhận không?
-   [ ] Có hiểu vì sao crash window có thể tạo duplicate không?
-   [ ] Có lưu payload event đủ để consumer xử lý mà không phải đoán
    trạng thái hiện tại không?
-   [ ] Có ghi log nhưng không lưu trạng thái bền vững cho việc retry
    không?
-   [ ] Có xử lý hai worker tranh cùng một row không, hoặc ghi rõ đây là
    giới hạn của POC?

## 6. Review Gate

Bạn cần giải thích và demo được: - Dual-write problem trước Outbox. -
Transaction giữa Product và Outbox. - Worker publish và trạng thái
outbox. - Kịch bản duplicate sau publish. - Cách Phase 5 sẽ xử lý
duplicate ở consumer.

**Bằng chứng cần lưu:** sơ đồ sequence, test rollback, test worker
restart và query trạng thái outbox.
