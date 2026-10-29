# Phase 5 --- Idempotent Consumer và đồng bộ một nguồn đến nhiều database

**Mục tiêu:** ghép Outbox với nhiều consumer/database đích; cho phép
message được giao lại nhưng không làm lặp side effect, đồng thời theo
dõi kết quả riêng của từng đích.

**Ước lượng:** 5--7 buổi.

## 0. Prerequisites

### Cần biết

-   Hoàn thành Outbox ở Phase 4.
-   Hiểu RabbitMQ routing và acknowledgement.
-   Hiểu retry và duplicate window.
-   EF Core transaction trong một database.

### Chưa cần biết

-   Saga orchestration.
-   Exactly-once toàn hệ thống.
-   Kubernetes hoặc distributed transactions.

## 1. Kiến thức cần học --- theo thứ tự

1.  At-least-once delivery và idempotent processing.
2.  Idempotency key / event ID và bảng `ProcessedMessages`.
3.  Transaction tại consumer: cập nhật dữ liệu đích và ghi nhận event đã
    xử lý trong cùng transaction.
4.  Fan-out: một event được route đến nhiều queue/consumer.
5.  Theo dõi trạng thái **theo từng đích**; một đích thành công không có
    nghĩa tất cả đích thành công.
6.  Event ordering, version/sequence number và stale event.
7.  Eventual consistency và reconciliation cơ bản.

Tài liệu: -
https://microservices.io/patterns/communication-style/idempotent-consumer.html -
https://www.rabbitmq.com/docs/exchanges -
https://www.rabbitmq.com/docs/confirms -
https://microservices.io/patterns/data/transactional-outbox.html

## 2. Đề bài tiny project --- One Source, Two Destinations

Mở rộng Phase 4 để Product thay đổi ở DB nguồn được đồng bộ tới hai
database đích độc lập: `DestinationB` và `DestinationC`.

ShopMini đã có Product Catalog chính. Hai hệ thống phụ thuộc cần bản sao dữ liệu sản phẩm:
- **Storefront DB (Destination B):** dữ liệu sản phẩm dùng để hiển thị trên website bán hàng.
- **Reporting DB (Destination C):** bản sao sản phẩm dùng cho báo cáo nội bộ.

Hai hệ thống cần nhận cùng thay đổi nhưng có thể gặp sự cố độc lập. Website không được chờ hệ thống báo cáo phục hồi mới cập nhật; ngược lại, một event được giao lại không được khiến cùng một thay đổi bị áp dụng lặp gây sai trạng thái hoặc lặp side effect.

### Quy tắc nghiệp vụ

- Khi sản phẩm được tạo/cập nhật/xóa ở nguồn, mỗi đích cần nhận event tương ứng.
- B và C có queue riêng, consumer riêng và database riêng; không được cấu hình thành hai consumer cạnh tranh trên cùng một queue nếu cả hai phải nhận mọi event.
- Mỗi event có `EventId` ổn định. Retry/redelivery giữ nguyên ID đó.
- Ở từng đích, cập nhật Product projection và lưu `ProcessedMessages` phải thuộc cùng transaction.
- Trạng thái đồng bộ phải được xem riêng cho B và C: `Pending`, `Processed`, `Retrying` hoặc `Failed` (tên cụ thể do bạn chọn và ghi rõ).
- Nếu B ngừng hoạt động, C vẫn tiếp tục xử lý. Khi B hồi phục, event chưa hoàn tất phải có thể được xử lý lại.
- Nếu dùng event version, event cũ không được ghi đè dữ liệu mới hơn.

### Kịch bản demo bắt buộc

1. Tạo `PRD-1001` ở nguồn → cả Storefront DB và Reporting DB có sản phẩm.
2. Đổi giá từ `25.00` sang `29.00` → cả hai đích hội tụ về giá mới.
3. Làm cho Destination C ngừng hoạt động, cập nhật giá thành `35.00` → B nhận giá mới, C chưa nhận; trạng thái phải thể hiện rõ C còn pending/retrying.
4. Bật lại C → C cập nhật thành `35.00`.
5. Gửi lại cùng `EventId` ít nhất hai lần → không tạo side effect lặp; chứng minh bằng dữ liệu/log.
6. Nếu hỗ trợ xóa, xóa `PRD-1001` ở nguồn và quy định rõ cách hai đích biểu diễn việc xóa.

Không cần giao diện web. Dùng SQL query, CLI hoặc endpoint chẩn đoán để chứng minh trạng thái.
"""

### Kiến trúc yêu cầu

-   Một nguồn: Product API + Outbox.
-   RabbitMQ exchange và hai queue đích.
-   Hai consumer độc lập, mỗi consumer có database riêng.
-   Có thể dùng cùng schema ở hai database đích để đơn giản hóa.

### Bảng ở mỗi database đích

`ProcessedMessages`: - `EventId` (PK hoặc unique constraint) -
`ProcessedAtUtc`

### Yêu cầu consumer

1.  Nhận event có `EventId`.
2.  Trong cùng transaction ở database đích:
    -   kiểm tra event đã xử lý chưa;
    -   nếu chưa, cập nhật Product projection và insert
        `ProcessedMessages`;
    -   commit.
3.  Nếu event đã được xử lý, không áp dụng side effect lần nữa; ghi log
    duplicate và ack theo thiết kế.
4.  Nếu transaction thất bại, không đánh dấu event đã xử lý; không ack
    như thành công.
5.  Mỗi consumer phải độc lập: database B tạm ngừng không được ngăn
    consumer C xử lý event.

### Theo dõi kết quả

Tạo bảng/trạng thái theo từng đích ở phía nguồn hoặc một phần
log/monitoring rõ ràng, để có thể phân biệt: - B đã xử lý. - C đã xử
lý. - B đang chờ / retry. - C đang chờ / retry. - Đích thất bại lâu dài
cần can thiệp.

Không dùng một cột `Sent = true` chung để tuyên bố mọi đích đã đồng bộ
nếu chưa có bằng chứng cho từng đích.

### Đầu vào

Tạo, sửa và xóa Product ở DB nguồn.

### Đầu ra

-   B và C cuối cùng có trạng thái đúng theo event contract.
-   Một đích có thể tạm lỗi trong khi đích còn lại vẫn xử lý.
-   Message gửi lại không gây duplicate side effect.
-   Trạng thái từng đích có thể tra cứu.

### Ràng buộc

-   Không dùng distributed transaction giữa ba database.
-   Không tuyên bố exactly-once delivery.
-   Định nghĩa rõ thứ tự sự kiện cần bảo đảm, nếu có.
-   Nếu dùng direct/fanout exchange, chứng minh mỗi consumer có queue
    riêng; không để hai consumer cạnh tranh cùng một queue khi mục tiêu
    là cả hai đều nhận event.

## 3. Acceptance criteria

-   [ ] Một event được route tới hai queue độc lập.
-   [ ] B và C có thể xử lý cùng event độc lập.
-   [ ] Gửi cùng `EventId` nhiều lần không lặp side effect ở từng đích.
-   [ ] Dữ liệu đích và `ProcessedMessages` được commit cùng
    transaction.
-   [ ] Một đích down không ngăn đích còn lại xử lý.
-   [ ] Có thể xem trạng thái riêng của từng đích.
-   [ ] Sau khi đích lỗi phục hồi, event pending có thể được xử lý lại.
-   [ ] Có test cho event duplicate và event đến sai thứ tự hoặc nêu rõ
    ordering chưa được bảo đảm.

## 4. Checkwork

### Câu hỏi giải thích

1.  Vì sao ack sau commit DB nhưng trước commit DB là hai lựa chọn khác
    nhau?
2.  Vì sao ghi `ProcessedMessages` sau khi update Product mà không cùng
    transaction vẫn có race/failure window?
3.  Nếu cập nhật Product thành công nhưng insert `ProcessedMessages`
    thất bại thì sao?
4.  Nếu insert `ProcessedMessages` thành công nhưng cập nhật Product
    thất bại thì sao?
5.  Fanout exchange khác gì hai consumer cùng đọc một queue?
6.  Nếu B đã xử lý nhưng C chưa xử lý, trạng thái tổng thể nên là gì?
7.  Nếu event version 12 tới trước version 11 thì xử lý ra sao?

### Bài code bổ sung

-   Thêm `Version` hoặc sequence number cho Product event.
-   Viết script kiểm tra sản phẩm có cùng version giữa các đích.
-   Thêm một event delete và xử lý tombstone/record deletion nhất quán.
-   Tạo endpoint hoặc command line để tra cứu trạng thái đồng bộ theo
    Product ID.

### Tình huống lỗi

-   Consumer cập nhật DB thành công nhưng chết trước ack.
-   Duplicate event được giao lại.
-   DB B tắt nhưng DB C vẫn hoạt động.
-   DB đích hồi phục sau một thời gian dài.
-   Event mới đến trước event cũ.
-   Một event không thể xử lý do schema/payload không hợp lệ.

## 5. Pitfalls --- tự kiểm tra

-   [ ] Có dùng event ID ổn định xuyên suốt retry không?
-   [ ] Có unique constraint thực sự ở DB hay chỉ query trước rồi hy
    vọng không trùng?
-   [ ] Có commit dữ liệu và deduplication record trong cùng transaction
    không?
-   [ ] Có nhầm fanout với competing consumers không?
-   [ ] Có một trạng thái chung che giấu việc một đích thất bại không?
-   [ ] Có giả định mọi message sẽ đến đúng thứ tự không?
-   [ ] Có giữ message lỗi vĩnh viễn trong vòng retry vô hạn không?

## 6. Review Gate

Bạn cần demo: - Một event tới B và C. - B xử lý thành công, C lỗi; sau
đó C phục hồi. - Một event được giao lặp nhưng không lặp side effect. -
Tra cứu được trạng thái đồng bộ theo từng đích. - Giải thích rõ hệ thống
đạt eventual consistency trong điều kiện nào và còn giới hạn gì.

**Bằng chứng cần lưu:** sơ đồ routing, schema bảng deduplication, test
duplicate, test destination outage và báo cáo trạng thái B/C.
