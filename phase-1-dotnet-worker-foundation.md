# Phase 1 --- Nền tảng .NET cho background worker

**Mục tiêu:** hiểu cách tổ chức một tiến trình backend chạy độc lập,
được DI quản lý, xử lý tác vụ bất đồng bộ, ghi log và thao tác database
bằng transaction.

**Ước lượng:** 3--5 buổi học, tùy mức độ quen với C#.

## 0. Prerequisites

### Cần biết

-   C# class, interface, constructor, `async` / `await`, `Task`,
    `try/catch`.
-   ASP.NET Core project và DI ở mức đã dùng
    `builder.Services.AddDbContext`.
-   SQL cơ bản: bảng, primary key, insert/select/update.
-   EF Core: `DbContext`, `SaveChangesAsync`.

### Chưa cần biết

-   Microservices architecture chuyên sâu.
-   RabbitMQ/Kafka.
-   Transactional Outbox, Saga.
-   Docker nâng cao, Kubernetes.
-   Distributed transactions.

Nếu chưa giải thích được `async/await`, `Task` hoặc constructor
injection, hãy dành một buổi ôn trước. Không cần học hết C#.

## 1. Kiến thức cần học --- theo thứ tự

1.  **DI và lifetime:** `Transient`, `Scoped`, `Singleton`; vì sao
    `DbContext` thường scoped và không thread-safe.
    -   https://learn.microsoft.com/en-us/dotnet/core/extensions/dependency-injection
    -   https://learn.microsoft.com/en-us/ef/core/dbcontext-configuration/
2.  **Generic Host và Worker Service:** tiến trình chạy nền khác API
    nhận HTTP request thế nào.
    -   https://learn.microsoft.com/en-us/dotnet/core/extensions/workers
3.  **Async và exception handling:** await tuần tự, exception
    propagation, cancellation token.
    -   https://learn.microsoft.com/en-us/dotnet/csharp/asynchronous-programming/
4.  **Logging và configuration:** `ILogger<T>`, `appsettings.json`,
    environment variables.
    -   https://learn.microsoft.com/en-us/dotnet/core/extensions/logging
5.  **EF Core transaction:** nhiều thay đổi trong cùng database được
    commit hoặc rollback cùng nhau.
    -   https://learn.microsoft.com/en-us/ef/core/saving/transactions

## 2. Đề bài tiny project --- Task Processing Worker

Tạo solution tên `SyncPoc`, trong đó có một Worker Service và một
database SQL Server (local hoặc container). Chưa dùng message broker.

### Bài toán

Worker lấy các công việc chưa xử lý từ bảng `WorkItems`, xử lý từng công
việc và ghi trạng thái.

Bạn đang làm backend cho **ShopMini**, một cửa hàng bán đồ công nghệ nhỏ. Nhân viên quản lý danh mục sản phẩm; về sau, khi một sản phẩm thay đổi, hệ thống sẽ cần gửi thông tin đó sang các hệ thống khác. Trước khi học message broker, bạn cần hiểu một tiến trình chạy nền có thể xử lý công việc đã được lưu trong database như thế nào.

Ở phase này, chưa đồng bộ giữa các hệ thống và chưa có RabbitMQ. `WorkItems` chỉ đại diện cho các tác vụ nội bộ mà ShopMini cần xử lý, ví dụ: “tạo bản ghi báo cáo cho sản phẩm vừa nhập” hoặc “kiểm tra dữ liệu sản phẩm có đủ trường bắt buộc”.

**Quy tắc nghiệp vụ:**
- Mỗi công việc có một ID duy nhất và một payload mô tả việc cần làm.
- Chỉ công việc ở trạng thái `Pending` mới được đưa vào xử lý.
- Một công việc hợp lệ được chuyển sang `Completed` sau khi xử lý và lưu kết quả.
- Một công việc không hợp lệ được chuyển sang `Failed`, lưu lý do và không làm worker dừng vĩnh viễn.
- Khi ứng dụng khởi động lại, công việc `Completed` không được chạy lại như một công việc mới.


- Seed ít nhất ba công việc:
1. `GenerateProductReport` với payload hợp lệ → `Completed`.
2. `ValidateProduct` với payload thiếu `ProductId` → `Failed`, có lý do.
3. Một công việc hợp lệ khác đặt sau công việc lỗi → vẫn được xử lý.

Đầu ra không phải giao diện web. Bằng chứng là các dòng trong database và log của worker.

### Bảng gợi ý

`WorkItems`: - `Id` (PK) - `Payload` - `Status` (`Pending`,
`Processing`, `Completed`, `Failed`) - `CreatedAtUtc` - `ProcessedAtUtc`
nullable - `ErrorMessage` nullable

### Yêu cầu

-   Khởi tạo project bằng `dotnet new worker`; tự gõ các lệnh để hiểu
    cấu trúc.
-   Tạo `WorkItem` entity và `DbContext`.
-   Đăng ký `DbContext` và các service bằng DI.
-   Worker định kỳ đọc một số công việc `Pending`, xử lý tuần tự, cập
    nhật trạng thái.
-   Mỗi công việc có exception handling riêng để một item lỗi không làm
    worker chết hoàn toàn.
-   Dùng `ILogger` ghi ID công việc, trạng thái, lỗi và thời gian xử lý.
-   Đọc cấu hình từ config/environment, không hard-code connection
    string có bí mật.
-   Dùng `CancellationToken` để tiến trình dừng có kiểm soát.

### Đầu vào

Các dòng `WorkItems` do bạn seed hoặc insert thủ công; trong đó có ít
nhất một item hợp lệ và một item được cố tình cấu hình để lỗi.

### Đầu ra

-   Item hợp lệ chuyển sang `Completed`.
-   Item lỗi chuyển sang `Failed`, có error log.
-   Worker tiếp tục xử lý item tiếp theo.
-   Khi dừng rồi chạy lại, worker không tự xử lý lại item đã
    `Completed`.

### Ràng buộc

-   Chưa dùng RabbitMQ.
-   Không thêm frontend.
-   Không catch exception rồi bỏ qua im lặng.
-   Không dùng `Thread.Sleep` trong luồng async; dùng cơ chế delay phù
    hợp và cancellation-aware.

## 3. Acceptance criteria

-   [ ] README có lệnh tạo/chạy project từ trạng thái sạch.
-   [ ] Có thể giải thích vì sao `DbContext` không nên được chia sẻ như
    singleton.
-   [ ] Worker xử lý được nhiều item mà không cần HTTP request.
-   [ ] Log phân biệt bắt đầu, thành công và thất bại.
-   [ ] Item đã hoàn tất không bị xử lý lại sau restart.
-   [ ] Shutdown không tạo exception không được xử lý chỉ vì
    cancellation bình thường.
-   [ ] Không commit secret/connection string nhạy cảm vào Git.

## 4. Checkwork

### Câu hỏi giải thích

1.  Khác nhau giữa API Controller và `BackgroundService` là gì?
2.  Vì sao `DbContext` thường đăng ký `Scoped` nhưng `BackgroundService`
    thường là singleton?
3.  Nếu worker singleton cần `DbContext`, vì sao không nên inject trực
    tiếp một scoped `DbContext` vào constructor của worker? Hãy tìm hiểu
    `IServiceScopeFactory`. (dụng ý bài này)
4.  Khi nào `await` tuần tự phù hợp hơn `Task.WhenAll`?
5.  Transaction giúp bảo đảm điều gì trong một database? Nó chưa giải
    quyết điều gì giữa database và message broker?

### Bài code bổ sung

-   Thêm giới hạn mỗi lượt xử lý tối đa 5 item.
-   Thêm số lần xử lý và thời điểm xử lý cuối.
-   Cho phép cấu hình khoảng nghỉ giữa hai lượt qua `appsettings.json`.

### Tình huống lỗi

-   Database tắt khi worker khởi động.
-   Một item có payload không hợp lệ.
-   Worker bị dừng giữa lúc xử lý.
-   Có hai worker cùng chạy --- hãy ghi nhận vì sao thiết kế hiện tại có
    thể khiến cùng một item bị lấy đồng thời; chưa cần giải quyết
    distributed locking hoàn chỉnh.

## 5. Pitfalls --- tự kiểm tra

-   [ ] Có phải `BackgroundService` tự tạo scope cho `DbContext` không?
-   [ ] Có phải bắt mọi exception rồi tiếp tục mà không log là xử lý lỗi
    tốt?
-   [ ] Có phải `SaveChangesAsync` tự bảo đảm cả quy trình nghiệp vụ
    nhiều bước đều atomic?
-   [ ] Có phải chỉ cần thêm `async` là code chạy song song?
-   [ ] Có phải `CancellationToken` nên bị bỏ qua?

## 6. Review Gate

Chỉ chuyển phase khi bạn có thể: - Vẽ luồng
`Host → Worker → DI → DbContext → Database`. - Giải thích lifetime của
worker và `DbContext`. - Demo một item thành công, một item lỗi và một
lần restart. - Chỉ ra ít nhất một failure mode chưa xử lý và cách bạn sẽ
kiểm tra nó.

**Bằng chứng cần lưu:** README, log mẫu, ảnh/chụp output hoặc test, sơ
đồ đơn giản.
