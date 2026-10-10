#!/usr/bin/env bash
# Load test script for flowguard using k6
# Usage: ./scripts/load-test.sh [options]
# Options:
#   -u, --url URL        Target URL (default: http://localhost:3000)
#   -d, --duration DURATION  Test duration (default: 30s)
#   -v, --vus VUS        Number of virtual users (default: 10)
#   -r, --rate RATE      Requests per second (default: 100)
#   -h, --help           Show this help

set -euo pipefail

URL="http://localhost:3000"
DURATION="30s"
VUS=10
RATE=100

while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--url)
            URL="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        -v|--vus)
            VUS="$2"
            shift 2
            ;;
        -r|--rate)
            RATE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -u, --url URL        Target URL (default: http://localhost:3000)"
            echo "  -d, --duration DURATION  Test duration (default: 30s)"
            echo "  -v, --vus VUS        Number of virtual users (default: 10)"
            echo "  -r, --rate RATE      Requests per second (default: 100)"
            echo "  -h, --help           Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check if k6 is available
if ! command -v k6 &> /dev/null; then
    echo "k6 not found. Installing via docker..."
    K6_CMD="docker run --rm -i grafana/k6:latest run -"
else
    K6_CMD="k6 run -"
fi

cat <<'EOF' | $K6_CMD --quiet
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');
const requestDuration = new Trend('request_duration');
const requestsTotal = new Counter('requests_total');

export const options = {
  scenarios: {
    constant_rate: {
      executor: 'constant-arrival-rate',
      rate: __ENV.RATE || 100,
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: __ENV.VUS || 10,
      maxVUs: __ENV.VUS || 10,
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<500'],
    'http_req_failed': ['rate<0.1'],
    'errors': ['rate<0.1'],
  },
};

const BASE_URL = __ENV.URL || 'http://localhost:3000';

export default function () {
  const payload = JSON.stringify({
    message: `test-message-${__VU}-${__ITER}`,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const start = new Date();
  const res = http.post(`${BASE_URL}/v1/send-message`, payload, params);
  const duration = new Date() - start;

  requestDuration.add(duration);
  requestsTotal.add(1);

  const success = check(res, {
    'status is 200': (r) => r.status === 200,
    'has trace id': (r) => r.headers['X-Request-Id'] !== undefined,
    'has echo': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.data && body.data.echo !== undefined;
      } catch {
        return false;
      }
    },
  });

  errorRate.add(!success);

  sleep(0.01); // Small sleep to prevent overwhelming
}

export function handleSummary(data) {
  return {
    'stdout': textSummary(data, { indent: ' ', enableColors: true }),
  };
}

function textSummary(data, options = {}) {
  const indent = options.indent || '';
  const enableColors = options.enableColors || false;

  let output = '\n';
  output += indent + '=========================\n';
  output += indent + 'LOAD TEST RESULTS SUMMARY\n';
  output += indent + '=========================\n\n';

  if (data.metrics.http_reqs) {
    output += indent + `Total Requests: ${data.metrics.http_reqs.values.count}\n`;
  }
  if (data.metrics.http_req_duration) {
    const d = data.metrics.http_req_duration.values;
    output += indent + `Request Duration:\n`;
    output += indent + `  avg: ${d.avg.toFixed(2)}ms\n`;
    output += indent + `  min: ${d.min.toFixed(2)}ms\n`;
    output += indent + `  max: ${d.max.toFixed(2)}ms\n`;
    output += indent + `  p(90): ${d['p(90)'].toFixed(2)}ms\n`;
    output += indent + `  p(95): ${d['p(95)'].toFixed(2)}ms\n`;
    output += indent + `  p(99): ${d['p(99)'].toFixed(2)}ms\n`;
  }
  if (data.metrics.http_req_failed) {
    output += indent + `Error Rate: ${(data.metrics.http_req_failed.values.rate * 100).toFixed(2)}%\n`;
  }
  if (data.metrics.errors) {
    output += indent + `Custom Error Rate: ${(data.metrics.errors.values.rate * 100).toFixed(2)}%\n`;
  }

  output += '\n';
  return output;
}
EOF