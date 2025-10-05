#...................................................SNS-SQS-Lambda message sending code.................................................
# import json
# import boto3

# sns = boto3.client("sns")
# TOPIC_ARN = "arn:aws:sns:ap-south-1:610726994339:mySNS.fifo"

# HTML_PAGE = """
# <!DOCTYPE html>
# <html lang="en">
# <head>
#   <meta charset="UTF-8" />
#   <meta name="viewport" content="width=device-width, initial-scale=1.0" />
#   <title>Send Message to SNS</title>
#   <style>
#     body {
#       font-family: Arial, sans-serif;
#       background: linear-gradient(to right, #667eea, #764ba2);
#       color: #fff;
#       display: flex;
#       justify-content: center;
#       align-items: center;
#       height: 100vh;
#       margin: 0;
#     }
#     .container {
#       background: rgba(255, 255, 255, 0.1);
#       padding: 40px;
#       border-radius: 12px;
#       box-shadow: 0 0 20px rgba(0,0,0,0.3);
#       text-align: center;
#       width: 90%;
#       max-width: 400px;
#     }
#     h2 {
#       margin-bottom: 20px;
#       color: #fff;
#     }
#     input[type="text"] {
#       width: 80%;
#       padding: 10px;
#       border-radius: 8px;
#       border: none;
#       outline: none;
#       margin-bottom: 20px;
#       font-size: 16px;
#       text-align: center;
#     }
#     button {
#       background-color: #4CAF50;
#       color: white;
#       padding: 10px 20px;
#       border: none;
#       border-radius: 8px;
#       font-size: 16px;
#       cursor: pointer;
#       transition: background 0.3s;
#     }
#     button:hover {
#       background-color: #45a049;
#     }
#     #result {
#       margin-top: 20px;
#       font-weight: bold;
#     }
#   </style>
#   <script>
#     async function sendMessage() {
#       const msg = document.getElementById("msg").value.trim();
#       const result = document.getElementById("result");
#       if (!msg) {
#         result.style.color = "yellow";
#         result.innerText = "⚠️ Please enter a message";
#         return;
#       }

#       result.style.color = "#fff";
#       result.innerText = "Sending...";

#       try {
#         const response = await fetch(window.location.href, {
#           method: "POST",
#           headers: { "Content-Type": "application/json" },
#           body: JSON.stringify({ message: msg })
#         });

#         const text = await response.text();
#         try {
#           const data = JSON.parse(text);
#           if (data.status === "Message sent") {
#             result.style.color = "lightgreen";
#             result.innerText = "✅ Message sent successfully!";
#           } else {
#             result.style.color = "orange";
#             result.innerText = "⚠️ " + (data.reason || "Unknown error");
#           }
#         } catch {
#           result.style.color = "red";
#           result.innerText = "❌ Unexpected response: " + text;
#         }
#       } catch (err) {
#         result.style.color = "red";
#         result.innerText = "❌ Error: " + err;
#       }
#     }
#   </script>
# </head>
# <body>
#   <div class="container">
#     <h2>🚀 Send Message to SNS (FIFO)</h2>
#     <input type="text" id="msg" placeholder="Enter your message here" />
#     <br />
#     <button onclick="sendMessage()">Send</button>
#     <p id="result"></p>
#   </div>
# </body>
# </html>
# """

# def lambda_handler(event, context):
#     try:
#         # Detect HTTP method (API Gateway HTTP API)
#         method = event.get("requestContext", {}).get("http", {}).get("method", "GET")

#         # Serve HTML page
#         if method == "GET":
#             return {
#                 "statusCode": 200,
#                 "headers": {
#                     "Content-Type": "text/html",
#                     "Access-Control-Allow-Origin": "*"
#                 },
#                 "body": HTML_PAGE
#             }

#         # Handle message submission
#         elif method == "POST":
#             body_raw = event.get("body", "{}")
#             try:
#                 body = json.loads(body_raw)
#             except Exception:
#                 body = {}

#             message = body.get("message", "").strip()

#             if not message:
#                 return {
#                     "statusCode": 400,
#                     "headers": {
#                         "Access-Control-Allow-Origin": "*",
#                         "Content-Type": "application/json"
#                     },
#                     "body": json.dumps({
#                         "status": "Error",
#                         "reason": "No message provided"
#                     })
#                 }

#             sns.publish(
#                 TopicArn=TOPIC_ARN,
#                 Message=message,
#                 Subject="Message from Lambda Webpage",
#                 MessageGroupId="web-message-group",
#                 MessageDeduplicationId=str(hash(message))
#             )

#             return {
#                 "statusCode": 200,
#                 "headers": {
#                     "Access-Control-Allow-Origin": "*",
#                     "Content-Type": "application/json"
#                 },
#                 "body": json.dumps({
#                     "status": "Message sent",
#                     "message": message
#                 })
#             }

#         # Invalid HTTP method
#         else:
#             return {
#                 "statusCode": 405,
#                 "headers": {"Access-Control-Allow-Origin": "*"},
#                 "body": "Method Not Allowed"
#             }

#     except Exception as e:
#         return {
#             "statusCode": 500,
#             "headers": {
#                 "Access-Control-Allow-Origin": "*",
#                 "Content-Type": "application/json"
#             },
#             "body": json.dumps({
#                 "status": "Internal Server Error",
#                 "error": str(e)
#             })
#         }


#.........................................................SQS-Lambda message Read code.................................................

import json
import boto3

# Create SQS client
sqs = boto3.client("sqs")

# Replace with your SQS queue URL
QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/610726994339/myQueue.fifo"

# HTML content served by Lambda
HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>View SQS Messages</title>
  <style>
    body {
      font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
      background: linear-gradient(to right, #667eea, #764ba2);
      color: white;
      height: 100vh;
      display: flex;
      justify-content: center;
      align-items: center;
      margin: 0;
    }
    .container {
      background: rgba(255, 255, 255, 0.1);
      padding: 40px;
      border-radius: 12px;
      text-align: center;
      box-shadow: 0 0 20px rgba(0, 0, 0, 0.3);
      width: 90%;
      max-width: 600px;
    }
    h2 {
      margin-bottom: 20px;
    }
    button {
      background-color: #4CAF50;
      color: white;
      border: none;
      padding: 10px 20px;
      border-radius: 8px;
      font-size: 16px;
      cursor: pointer;
      transition: background 0.3s;
    }
    button:hover {
      background-color: #45a049;
    }
    #messages {
      margin-top: 20px;
      text-align: left;
      background: rgba(255,255,255,0.1);
      padding: 15px;
      border-radius: 8px;
      max-height: 300px;
      overflow-y: auto;
      white-space: pre-wrap;
    }
  </style>
  <script>
    async function loadMessages() {
      const resultBox = document.getElementById("messages");
      resultBox.innerText = "Loading messages...";
      try {
        const response = await fetch(window.location.href, { method: "POST" });
        const text = await response.text();
        try {
          const data = JSON.parse(text);
          if (data.messages && data.messages.length > 0) {
            resultBox.innerHTML = "<strong>Messages:</strong><br><br>" +
              data.messages.map((m, i) => (i+1) + ". " + m).join("<br><br>");
          } else {
            resultBox.innerText = "No messages available.";
          }
        } catch {
          resultBox.innerText = "Response: " + text;
        }
      } catch (err) {
        resultBox.innerText = "Error: " + err;
      }
    }
  </script>
</head>
<body>
  <div class="container">
    <h2>📬 View Messages from SQS</h2>
    <button onclick="loadMessages()">Load Messages</button>
    <div id="messages"></div>
  </div>
</body>
</html>
"""

def lambda_handler(event, context):
    try:
        # Detect HTTP method (API Gateway)
        method = event.get("requestContext", {}).get("http", {}).get("method", "GET")

        # Serve HTML on GET
        if method == "GET":
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "text/html",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": HTML_PAGE
            }

        # On POST, read from SQS
        elif method == "POST":
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=1
            )

            messages = []
            if "Messages" in response:
                for msg in response["Messages"]:
                    messages.append(msg["Body"])
                    # Delete message after reading
                    sqs.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )

            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Content-Type": "application/json"
                },
                "body": json.dumps({"messages": messages})
            }

        else:
            return {
                "statusCode": 405,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "Method not allowed"})
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({"error": str(e)})
        }
