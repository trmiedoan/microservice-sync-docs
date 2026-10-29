# Phase 2 --- RabbitMQ: producer, queue và consumer

**Mục tiêu:** tạo được luồng message bất đồng bộ giữa hai tiến trình độc
lập và giải thích được broker đang làm gì.

**Ước lượng:** 3--5 buổi.

## 0. Prerequisites

### Cần biết

-   Hoàn thành Phase 1 ở mức cơ bản.
-   C# async/await, DI, logging và cấu hình.
-   Biết cách chạy một ứng dụng .NET và kiểm tra log.

### Chưa cần biết

-   Transactional Outbox.
-   Exactly-once processing.
-   Saga.
-   Cluster RabbitMQ hoặc Kafka.

## 1. Kiến thức cần học --- theo thứ tự

1.  Message broker giải quyết vấn đề gì; khác gì HTTP request trực tiếp.
2.  Producer, consumer, message, queue.
3.  Exchange, binding, routing key; default exchange và direct/fanout
    exchange.
4.  Consumer acknowledgement: ack/nack/reject và vì sao không nên ack
    trước khi xử lý xong.
5.  Durable queue và persistent message; hiểu giới hạn của chúng.
6.  Cách chạy RabbitMQ local và dùng Management UI.

Tài liệu: - https://www.rabbitmq.com/tutorials -
https://www.rabbitmq.com/docs/exchanges -
https://www.rabbitmq.com/docs/queues -
https://www.rabbitmq.com/docs/consumers

## 2. Đề bài tiny project --- Notification Queue

Tạo hai project .NET độc lập: - `NotificationProducer`: gửi thông báo
vào RabbitMQ. - `NotificationConsumer`: nhận và ghi log thông báo.

### Message contract

Tự thiết kế một DTO `NotificationMessage` có tối thiểu: - `MessageId`
(UUID) - `CreatedAtUtc` - `Recipient` - `Content`

ShopMini có một chức năng đặt hàng. Khi khách đặt hàng, hệ thống cần gửi thông báo xác nhận đơn hàng. Bạn chưa xây toàn bộ hệ thống đặt hàng; bạn đang xây **đường truyền thông báo độc lập** để học cách hai tiến trình giao tiếp qua RabbitMQ.

- `NotificationProducer` đóng vai trò một phần của Order API: tạo thông báo xác nhận đơn.
- `NotificationConsumer` đóng vai trò Notification Service: nhận thông báo và ghi nhận rằng nó đã được tiếp nhận/xử lý trong bản demo.
- Hai tiến trình có thể được chạy, dừng và khởi động lại độc lập.

### Quy tắc nghiệp vụ

- Khi nhân viên hoặc lệnh console tạo một thông báo xác nhận đơn, message phải có `MessageId`, `OrderId`, `Recipient` và `Content`.
- Consumer kiểm tra các trường bắt buộc trước khi xử lý.
- Consumer chỉ ack sau khi đã xử lý thành công theo phạm vi demo.
- Nếu consumer tạm dừng, message có thể chờ trong queue thay vì yêu cầu producer gọi trực tiếp consumer qua HTTP.
- Ở phase này, “xử lý thành công” chỉ có nghĩa consumer đã validate và ghi log kết quả; chưa gửi email/SMS thật.

### Kịch bản demo bắt buộc

Tạo hai message:
- `OrderId = ORD-1001`, recipient hợp lệ, content “Đơn hàng ORD-1001 đã được tiếp nhận” → consumer log thành công.
- Message thiếu recipient → consumer log lỗi validation, không được giả vờ xử lý thành công.

Chạy producer khi consumer đang tắt, sau đó bật consumer và quan sát queue. Ghi lại kết quả thực tế; đừng tự giả định message sẽ được lưu nếu queue chưa được khai báo/bind đúng.


### Yêu cầu

-   Chạy RabbitMQ local; có thể dùng Docker Compose nếu đã biết Docker.
-   Producer nhận payload từ console hoặc endpoint đơn giản rồi publish.
-   Consumer chạy độc lập, subscribe queue và log message.
-   Dùng JSON serialization; xác thực message có đủ field bắt buộc.
-   Consumer chỉ ack sau khi đã xử lý thành công.
-   Nếu message không hợp lệ, ghi log rõ ràng; chưa cần thiết kế retry
    phức tạp ở phase này.
-   Không để consumer phụ thuộc vào bộ nhớ của producer.

### Đầu vào

Người dùng nhập recipient và content hoặc producer tạo message mẫu.

### Đầu ra

Consumer nhận được message, log `MessageId`, recipient và kết quả xử lý.

### Ràng buộc

-   Chưa dùng database nghiệp vụ.
-   Chưa triển khai Outbox.
-   Chưa tuyên bố hệ thống bảo đảm không mất message chỉ vì queue
    durable.
-   Không dùng `Task.Delay` để giả lập một broker.

## 3. Acceptance criteria

-   [ ] Producer và consumer chạy ở hai process riêng.
-   [ ] Producer có thể publish khi consumer đang chạy.
-   [ ] Consumer có thể được khởi động lại mà producer không cần thay
    đổi code.
-   [ ] Message contract có `MessageId`.
-   [ ] Ack diễn ra sau xử lý thành công.
-   [ ] README có cách chạy RabbitMQ, producer và consumer.
-   [ ] Có thể giải thích exchange, queue, binding và routing key bằng
    sơ đồ.

## 4. Checkwork

### Câu hỏi giải thích

1.  Vì sao producer không cần biết địa chỉ HTTP của consumer?
2.  Exchange và queue khác nhau thế nào?
3.  Nếu consumer chết trước khi ack thì có thể xảy ra điều gì?
4.  Durable queue và persistent message giải quyết những vấn đề gì, và
    không giải quyết được gì?
5.  Vì sao broker nhận message không đồng nghĩa với database đích đã cập
    nhật?

### Bài code bổ sung

-   Tạo fanout exchange để gửi cùng một thông báo tới hai queue consumer
    khác nhau.
-   Thêm trường `MessageType` và `SchemaVersion`.
-   Thử publish một message với JSON thiếu field bắt buộc.

### Tình huống lỗi

-   Producer chạy khi RabbitMQ tắt.
-   Consumer tắt trong khi queue có message.
-   Consumer xử lý xong nhưng bị dừng trước ack.
-   Gửi JSON không hợp lệ.

Ghi lại hiện tượng thật quan sát được; không đoán dựa trên tên tính
năng.

## 5. Pitfalls --- tự kiểm tra

-   [ ] Có nhầm lẫn giữa exchange và queue không?
-   [ ] Có ack trước khi xử lý xong không?
-   [ ] Có coi log "published" ở ứng dụng là bằng chứng broker đã xác
    nhận không?
-   [ ] Có hard-code credential RabbitMQ vào source không?
-   [ ] Có cho rằng message luôn được giao đúng một lần không?

## 6. Review Gate

Bạn cần demo được: - Producer/consumer chạy độc lập. - Hai queue nhận
cùng một event qua fanout exchange. - Consumer bị dừng và khởi động
lại. - Giải thích được các giới hạn của demo và vì sao cần Phase 3.

**Bằng chứng cần lưu:** sơ đồ exchange/queue, message JSON mẫu, log
producer/consumer và README.
