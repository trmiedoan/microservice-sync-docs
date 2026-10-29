# Phase 3 --- Delivery reliability: confirms, retry và dead-letter queue

**Mục tiêu:** phân biệt publish, broker confirmation và consumer
processing; thiết kế retry có giới hạn và xử lý message lỗi mà không tạo
vòng lặp vô hạn.

**Ước lượng:** 4--6 buổi.

## 0. Prerequisites

### Cần biết

-   Producer/consumer và queue ở Phase 2.
-   Exception handling, logging và cấu hình.
-   Hiểu rằng một message có thể được giao lại.

### Chưa cần biết

-   Outbox.
-   Idempotent consumer hoàn chỉnh.
-   Saga.
-   Exactly-once end-to-end.

## 1. Kiến thức cần học --- theo thứ tự

1.  Publisher confirms: publisher biết broker đã nhận publish ở mức
    broker.
2.  Consumer acknowledgements: consumer xác nhận xử lý message.
3.  At-most-once, at-least-once và ý nghĩa thực tế.
4.  Retry có giới hạn, exponential backoff và jitter ở mức khái niệm.
5.  Dead-letter exchange/queue (DLX/DLQ) và phân biệt lỗi tạm thời với
    lỗi vĩnh viễn.
6.  Connection recovery, timeout và cancellation.
7.  Quan sát message bị redelivered và tránh retry nóng liên tục.

Tài liệu: - https://www.rabbitmq.com/docs/confirms -
https://www.rabbitmq.com/docs/consumers -
https://www.rabbitmq.com/docs/dlx - https://www.rabbitmq.com/tutorials

## 2. Đề bài tiny project --- Resilient Message Processor

Nâng cấp Notification Queue từ Phase 2.

Tiếp tục ShopMini và luồng thông báo xác nhận đơn hàng ở Phase 2. Thực tế, nếu khách đặt hàng thành công nhưng thông báo bị mất hoặc hệ thống retry vô hạn, khách có thể không nhận được xác nhận hoặc nhận nhiều thông báo giống nhau. Phase này chỉ tập trung vào **độ tin cậy của việc gửi/nhận message**; chưa giải quyết side effect trùng lặp hoàn chỉnh bằng idempotency.

### Quy tắc nghiệp vụ

- Producer cần phân biệt “đã thử publish” với “broker đã xác nhận nhận message”.
- Nếu kết nối tới RabbitMQ lỗi, producer ghi nhận kết quả không chắc chắn và retry theo giới hạn đã chọn.
- Consumer ack chỉ sau khi hoàn tất bước xử lý của bản demo.
- Lỗi tạm thời (ví dụ dịch vụ phụ thuộc tạm không truy cập được) có thể retry có giới hạn.
- Lỗi vĩnh viễn (ví dụ JSON không parse được hoặc thiếu `OrderId`) không nên retry vô hạn; đưa vào DLQ hoặc đường xử lý lỗi đã cấu hình.
- Mỗi lần thử cần theo dõi `MessageId`, attempt number, thời gian và nguyên nhân lỗi.

### Kịch bản demo bắt buộc

Dùng ba message:
1. Message hợp lệ → xử lý thành công.
2. Message mô phỏng lỗi tạm thời, sau một vài lần thử thì thành công → có log các lần retry.
3. Message sai schema → không retry vô hạn, có thể tìm thấy trong DLQ hoặc nơi lưu lỗi.

Ngoài ra, dừng RabbitMQ khi producer publish rồi bật lại. Ghi rõ trạng thái quan sát được và giới hạn của thiết kế; không tuyên bố rằng timeout chứng minh message chưa tới broker.
"""


### Yêu cầu phía producer

-   Bật publisher confirms theo hướng dẫn client RabbitMQ đang dùng.
-   Log `MessageId`, thời điểm publish và kết quả confirmation.
-   Nếu publish không được xác nhận hoặc xảy ra lỗi kết nối, xử lý theo
    chiến lược retry có giới hạn hoặc để lại trạng thái cần thử lại.
-   Không giả định timeout luôn có nghĩa broker chưa nhận message; trạng
    thái có thể không chắc chắn.

### Yêu cầu phía consumer

-   Ack sau khi xử lý thành công.
-   Khi lỗi tạm thời, thử lại theo chiến lược được mô tả rõ.
-   Giới hạn số lần retry.
-   Khi lỗi vĩnh viễn hoặc hết retry, chuyển message sang DLQ hoặc một
    đường xử lý lỗi được cấu hình rõ.
-   Log `MessageId`, số lần thử, loại lỗi và trạng thái cuối.

### Đầu vào

Message hợp lệ, message cố tình gây lỗi tạm thời và message lỗi vĩnh
viễn.

### Đầu ra

-   Message thành công được xử lý.
-   Lỗi tạm thời có thể được retry.
-   Lỗi kéo dài không tạo retry vô hạn.
-   Message lỗi cuối cùng có thể được kiểm tra riêng trong DLQ/log.

### Ràng buộc

-   Chưa thêm business database.
-   Không `catch` rồi ack mọi message như thể thành công.
-   Không retry vô hạn ngay lập tức trong vòng lặp.
-   Không gọi publisher confirmation là bằng chứng consumer đã xử lý.

## 3. Acceptance criteria

-   [ ] Có phân biệt publish attempt và broker confirm trong log.
-   [ ] Có retry limit được cấu hình.
-   [ ] Có backoff hoặc cơ chế trì hoãn retry phù hợp.
-   [ ] Message lỗi vĩnh viễn không bị retry vô hạn.
-   [ ] Có đường quan sát message thất bại (DLQ hoặc trạng thái lỗi rõ
    ràng).
-   [ ] Có test mô phỏng RabbitMQ tắt rồi bật lại.
-   [ ] README mô tả rõ chiến lược retry và các giới hạn.

## 4. Checkwork

### Câu hỏi giải thích

1.  Publisher confirm và consumer ack khác nhau thế nào?
2.  Nếu publisher timeout sau publish, làm sao biết message có thể đã
    tới broker hay chưa?
3.  Vì sao retry có thể gây duplicate?
4.  Khi nào nên retry và khi nào nên chuyển DLQ?
5.  Vì sao một retry loop không giới hạn có thể làm hệ thống tệ hơn?
6.  At-least-once có nghĩa gì với logic cập nhật database?

### Bài code bổ sung

-   Cấu hình max retry bằng appsettings.
-   Thêm timestamp và attempt number vào log.
-   Tạo công cụ/script để đọc message từ DLQ và cho phép requeue thủ
    công ở môi trường local.

### Tình huống lỗi

-   Broker dừng ngay trước publish.
-   Broker nhận publish nhưng kết nối mất trước khi producer nhận
    confirm.
-   Consumer xử lý xong nhưng chết trước ack.
-   Message luôn gây exception.
-   DLQ không được cấu hình hoặc routing sai.

## 5. Pitfalls --- tự kiểm tra

-   [ ] Có nhầm publisher confirm với consumer ack không?
-   [ ] Có cho rằng timeout nghĩa là message chắc chắn chưa được gửi
    không?
-   [ ] Có retry mọi lỗi, kể cả payload sai vĩnh viễn không?
-   [ ] Có để retry message ngay lập tức khiến queue bị nóng không?
-   [ ] Có quên rằng message redelivery có thể tạo duplicate side effect
    không?

## 6. Review Gate

Bạn cần trình diễn được ít nhất ba failure scenario, có log cho từng
trạng thái, và giải thích: - Khi nào hệ thống biết broker đã nhận
message. - Khi nào consumer được phép ack. - Message lỗi cuối cùng đi
đâu. - Vì sao vẫn cần idempotency dù đã có retry và confirms.

**Bằng chứng cần lưu:** test matrix, log của các tình huống lỗi và sơ đồ
retry/DLQ.
