import json
import boto3

sns = boto3.client('sns')
TOPIC_ARN = "arn:aws:sns:ap-south-1:716145636894:myfifotopic.fifo"

HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
  <title>Send Message to SNS</title>
  <script>
    async function sendMessage() {
      let msg = document.getElementById("msg").value;
      if (!msg) {
        alert("Please enter a message");
        return;
      }

      try {
        let response = await fetch(window.location.href, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ message: msg })
        });

        let contentType = response.headers.get("content-type") || "";
        let data;

        if (contentType.includes("application/json")) {
          data = await response.json();
          document.getElementById("result").innerText =
            data.status + ": " + (data.message || "");
        } else {
          let text = await response.text();
          document.getElementById("result").innerText = "Response: " + text;
        }
      } catch (err) {
        document.getElementById("result").innerText = "Error: " + err;
      }
    }
  </script>
</head>
<body>
  <h2>Send Message to SNS (FIFO)</h2>
  <input type="text" id="msg" placeholder="Enter your message">
  <button onclick="sendMessage()">Send</button>
  <p id="result" style="color:green; font-weight:bold;"></p>
</body>
</html>
"""

def lambda_handler(event, context):
    try:
        method = event.get("requestContext", {}).get("http", {}).get("method", "GET")

        if method == "GET":
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "text/html",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": HTML_PAGE
            }

        elif method == "POST":
            body_raw = event.get("body", "{}")
            try:
                body = json.loads(body_raw)
            except Exception:
                body = {}

            message = body.get("message", "")

            if message:
                sns.publish(
                    TopicArn=TOPIC_ARN,
                    Message=message,
                    Subject="Message from Lambda Webpage",
                    MessageGroupId="web-message-group",  # FIFO required
                    MessageDeduplicationId=str(hash(message))  # ensure deduplication
                )
                return {
                    "statusCode": 200,
                    "headers": {
                        "Access-Control-Allow-Origin": "*",
                        "Content-Type": "application/json"
                    },
                    "body": json.dumps({"status": "Message sent", "message": message})
                }
            else:
                return {
                    "statusCode": 400,
                    "headers": {
                        "Access-Control-Allow-Origin": "*",
                        "Content-Type": "application/json"
                    },
                    "body": json.dumps({"status": "Error", "reason": "No message provided"})
                }

        else:
            return {
                "statusCode": 405,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": "Method Not Allowed"
            }

    except Exception as e:
        # Catch-all error handling
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({"status": "Internal Server Error", "error": str(e)})
        }
