TS=$(date +%s%N)

curl -v "http://localhost:8086/api/v2/write?org=myorg&bucket=db" \
  --header "Authorization: Token sec-token" \
  --header "Content-Type: text/plain; charset=utf-8" \
  --data-binary "payments,payment_processor_id=D amount=19.9 $TS"

# curl -v "http://localhost:8086/api/v2/query?org=myorg" \
#     --header 'Content-Type: application/vnd.flux' \
#     --header 'Accept: application/json' \
#     --header 'Authorization: Token sec-token' \
#     -d 'from(bucket: "db")
#         |> range(start: -5m, stop: now())
#         |> filter(fn: (r) => r._measurement == "payments")'

curl -v "http://localhost:8086/api/v2/query?org=myorg" \
    --header 'Content-Type: application/vnd.flux' \
    --header 'Accept: application/json' \
    --header 'Authorization: Token sec-token' \
    -d 'from(bucket: "db")
        |> range(start: -5m)
        |> filter(fn: (r) => r._measurement == "payments")
        |> group(columns: ["payment_processor_id"])
        |> aggregateWindow(
            every: v.windowPeriod,
            fn: (tables) => tables
                |> map(fn: (r) => ({
                    r with
                    total_requests: 1.0,
                    total_amount: r._value
                }))
                |> sum(columns: ["total_requests", "total_amount"])
            )
        |> yield(name: "result")'



echo "payments,payment_processor_id=D amount=19.9 $TS"