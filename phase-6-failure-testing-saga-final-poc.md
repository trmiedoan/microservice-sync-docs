# Phase 6 --- Failure testing, vận hành POC và Saga

**Mục tiêu:** chứng minh POC có thể phục hồi sau lỗi, quan sát được
trạng thái và có tài liệu chạy/khắc phục. Hiểu Saga đủ để quyết định khi
nào cần dùng; không thêm Saga nếu bài toán không có workflow nghiệp vụ
phù hợp.

**Ước lượng:** 5--8 buổi. Đây là phase tổng hợp, có thể dài hơn nếu phải
sửa kiến trúc.

## 0. Prerequisites

### Cần biết

- Outbox, retry, publisher confirms.
- Idempotent consumer.
- Fan-out nhiều database.
- Eventual consistency, transaction và log.

### Chưa cần biết

- Kubernetes production.
- Multi-region deployment.
- Distributed tracing stack phức tạp.
- Saga framework cụ thể.

## 1. Kiến thức cần học --- theo thứ tự

1.  Failure matrix: liệt kê component, failure mode, trạng thái bền vững
    và cách phục hồi.
2.  Observability: structured logs, correlation ID, event ID, metrics cơ
    bản.
3.  Retry policy, DLQ inspection, replay và reconciliation.
4.  Idempotency và giới hạn của "exactly once".
5.  Saga: local transaction, event/command, orchestration/choreography
    và compensating action.
6.  Quyết định có cần Saga hay không: chỉ cần nếu có quy trình nghiệp vụ
    nhiều bước qua nhiều service, cần phản ứng/bù trừ khi một bước thất
    bại.

Tài liệu: - https://microservices.io/patterns/data/saga.html -
https://microservices.io/patterns/data/transactional-outbox.html -
https://www.rabbitmq.com/docs/dlx -
https://learn.microsoft.com/en-us/dotnet/core/extensions/logging

## 2. Đề bài tiny project --- Final Sync POC

Hoàn thiện hệ thống nguồn Product → RabbitMQ → Destination B và C.

Đây là bản bàn giao POC cho ShopMini: **Product Catalog chính → RabbitMQ → Storefront DB và Reporting DB**. Nhân viên cập nhật tên/giá sản phẩm ở nguồn; website và báo cáo nội bộ cần hội tụ về cùng dữ liệu. Người review (mentor) cần xác minh không chỉ luồng thành công mà cả hành vi khi một thành phần tắt, event được giao lặp hoặc trạng thái đồng bộ bị lệch.

Phạm vi chính là đồng bộ dữ liệu sản phẩm. Saga được đưa vào như bài tập thiết kế riêng, vì Saga thường phục vụ quy trình nghiệp vụ nhiều bước như đặt hàng–giữ tồn kho–thanh toán, không phải mặc định là một phần của data sync.

### Cam kết nghiệp vụ cần kiểm chứng

- Sau khi Product API xác nhận lưu thành công, event tương ứng tồn tại bền vững trong nguồn/outbox.
- Nếu broker hoặc một đích tạm thời không hoạt động, lỗi được quan sát và có cách phục hồi; đích khỏe mạnh vẫn xử lý độc lập.
- Khi lỗi tạm thời đã được khắc phục và retry/replay được thực hiện an toàn, các đích hợp lệ cuối cùng hội tụ về phiên bản sản phẩm mới nhất.
- Cùng một event có thể được giao nhiều lần, nhưng không được áp dụng side effect trùng ở từng đích.
- Hệ thống phải cho phép truy vết theo `EventId` và biết đích nào đã xử lý, còn chờ hay thất bại.
- Nếu có event sai thứ tự, chính sách version/order phải được xác định và kiểm thử; nếu chưa xử lý, phải ghi rõ đó là giới hạn.
- Không hứa “không bao giờ mất message” hoặc “exactly once” một cách tuyệt đối. Mô tả chính xác cơ chế, giả định và khoảng lỗi còn lại.

### Kịch bản demo nghiệp vụ cuối

Tạo `PRD-1001` tên “USB-C Hub”, giá `25.00`; xác nhận hai đích có cùng version. Cập nhật giá thành `29.00`, rồi chủ động dừng RabbitMQ hoặc một destination ở các thời điểm khác nhau. Thu thập log và trạng thái, bật lại thành phần, chạy retry/replay theo hướng dẫn, rồi chứng minh dữ liệu hội tụ mà không áp dụng lặp side effect. Cuối cùng, tạo một chênh lệch có chủ đích ở Destination C và chứng minh bước reconciliation phát hiện được.
"""

### Yêu cầu A: failure matrix

Tạo bảng có các cột: - Scenario ID - Điểm lỗi - Kỳ vọng - Trạng thái/log
mong đợi - Cách phục hồi - Kết quả thực tế - Pass/Fail

Bắt buộc có các scenario: 1. RabbitMQ tắt khi outbox worker publish. 2.
Worker chết sau khi broker nhận message nhưng trước khi đánh dấu outbox
published. 3. Consumer chết sau commit DB đích nhưng trước ack. 4.
Database B down, C vẫn up. 5. Database đích phục hồi sau khi hết retry
tạm thời. 6. Message payload không hợp lệ. 7. Duplicate event. 8. Event
sai thứ tự. 9. Service khởi động lại trong lúc còn outbox pending. 10.
Một event được route sai hoặc không tới queue đích.

### Yêu cầu B: quan sát và khôi phục

- Có `EventId` xuyên suốt log.
- Log có service/consumer, trạng thái, attempt count, timestamp và
  lỗi.
- Có cách kiểm tra outbox pending/failed, DLQ và trạng thái từng đích.
- Có hướng dẫn replay/reprocess an toàn; không tự ý xóa trạng thái
  deduplication để ép chạy lại.
- Có quy trình reconciliation cơ bản: so sánh ID/version giữa nguồn và
  đích để phát hiện chênh lệch.

### Yêu cầu C: tài liệu bàn giao

- Sơ đồ kiến trúc.
- Cách chạy từ trạng thái sạch.
- Các biến môi trường cần cấu hình.
- Test plan và kết quả thực tế.
- Failure modes chưa xử lý.
- Quyết định thiết kế và trade-offs.
- Hướng dẫn dừng/khởi động từng thành phần để tái hiện lỗi.

### Yêu cầu D: bài tập Saga

Không cần tích hợp Saga vào sync pipeline nếu không có use case nghiệp
vụ. Hãy làm một bài thiết kế ngắn: - Workflow ví dụ: tạo đơn hàng → giữ
tồn kho → thanh toán. - Chỉ ra mỗi bước thuộc service nào và transaction
nào. - Chọn một bước thất bại và thiết kế compensating action. - So sánh
orchestration với choreography. - Giải thích vì sao Saga không phải cơ
chế thay thế Outbox hoặc idempotency.

Nếu mentor xác nhận POC thực sự cần Saga, hãy mở rộng thành mini project
riêng sau khi review thiết kế; đừng tự động thêm Saga vào luồng đồng bộ
dữ liệu chỉ để dùng đủ concept.

## 3. Acceptance criteria

- [ ] Có failure matrix với kết quả thực tế, không chỉ dự đoán.
- [ ] Các scenario bắt buộc đều được chạy hoặc được ghi rõ vì sao chưa
      chạy được.
- [ ] Có thể theo dõi một `EventId` từ nguồn tới từng đích.
- [ ] Outbox pending không bị mất khi service restart.
- [ ] Duplicate không gây lặp side effect.
- [ ] Đích lỗi có thể phục hồi và hội tụ về trạng thái mong muốn.
- [ ] Message không hợp lệ có đường xử lý rõ ràng.
- [ ] Có kiểm tra/reconciliation để phát hiện chênh lệch.
- [ ] README hướng dẫn người khác chạy được trên máy sạch.
- [ ] Tài liệu nói rõ các cam kết và giới hạn; không quảng bá "exactly
      once" nếu chưa chứng minh được.
- [ ] Có bài thiết kế Saga và giải thích khi nào không cần dùng Saga.

## 4. Checkwork

### Câu hỏi giải thích

1.  "Broker nhận message", "consumer ack" và "database đích commit" khác
    nhau thế nào?
2.  Tại sao hệ thống có thể cần chấp nhận duplicate nhưng vẫn tránh
    duplicate side effect?
3.  Outbox đảm bảo điều gì và không đảm bảo điều gì?
4.  Nếu một đích đã xử lý còn một đích chưa xử lý, làm sao xác định hệ
    thống đã đồng bộ xong?
5.  Nếu event mới đến trước event cũ, hệ thống bảo vệ tính đúng đắn bằng
    cách nào?
6.  Khi nào retry là sai lựa chọn vì lỗi là vĩnh viễn?
7.  Reconciliation phát hiện được loại lỗi nào mà log bình thường có thể
    bỏ sót?
8.  Saga giải quyết loại bài toán nào? Một luồng đồng bộ dữ liệu đơn
    giản có bắt buộc cần Saga không?
9.  Vì sao một Saga cần idempotent handlers và cơ chế lưu trạng thái?
10. Bạn sẽ nói với mentor điều gì mà POC hiện chưa thể bảo đảm?

### Bài code bổ sung

- Thêm correlation ID cho một thao tác nghiệp vụ.
- Viết report tổng hợp số event pending/published/failed và trạng thái
  theo đích.
- Viết script tạo failure scenario theo từng bước.
- Thêm test reconciliation phát hiện một Product bị thiếu ở DB C.
- Nếu được yêu cầu, triển khai Saga demo riêng có một compensating
  action.

## 5. Pitfalls --- tự kiểm tra

- [ ] Có coi "không có exception" là bằng chứng dữ liệu đã đồng bộ?
- [ ] Có coi log là nguồn trạng thái bền vững duy nhất?
- [ ] Có retry mãi một message không hợp lệ?
- [ ] Có reprocess event mà không xem xét idempotency?
- [ ] Có xóa deduplication record khi replay khiến side effect chạy
      lại?
- [ ] Có dùng Saga để giải quyết vấn đề mà Outbox đã giải quyết?
- [ ] Có tuyên bố exactly-once mà không nêu phạm vi?
- [ ] Có bỏ qua reconciliation vì "RabbitMQ đã durable"?

## 6. Review Gate --- Final demo

Bạn sẵn sàng trình bày POC khi có thể: - Vẽ kiến trúc và sequence của
một event. - Chạy demo thành công và ít nhất ba failure scenario. - Tra
cứu trạng thái theo EventId và theo từng database đích. - Giải thích
duplicate window, retry, ack, outbox và idempotency. - Nêu điều kiện để
dữ liệu cuối cùng hội tụ. - Nêu rõ những gì chưa bảo đảm và hướng cải
thiện tiếp theo. - Giải thích vì sao Saga có hoặc không có vai trò trong
phạm vi bài toán này.

**Đầu ra cuối:** source code, README, sơ đồ, test matrix, log/test
evidence và danh sách giới hạn còn lại.
