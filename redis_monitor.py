import redis
import json
import sys
import socket
import requests
from datetime import datetime
from typing import Dict, List
import time

class RedisMonitor:
    def __init__(self, redis_configs: List[Dict]):
        self.redis_configs = redis_configs
        self.teams_webhook = "https://vietmapcorp.webhook.office.com/webhookb2/205670f3-463f-4ac3-85f8-f84b9f0da76e@fc2e159c-528b-4132-b3c0-f43226646ad7/JenkinsCI/74682a3b843b46529b662e5d4b85f65e/e1fe988a-0959-44ad-889d-44f6a1637286"

    def check_index_info(self, client: redis.Redis, idx_name: str) -> Dict:
        """Get information about a RediSearch index"""
        try:
            # RediSearch FT.INFO command
            info = client.execute_command(f'FT.INFO {idx_name}')
            if not info:
                return {"status": "not found"}
            
            # Convert response to dictionary
            info_dict = {}
            for i in range(0, len(info), 2):
                key = info[i].decode() if isinstance(info[i], bytes) else info[i]
                value = info[i + 1]
                if isinstance(value, bytes):
                    value = value.decode()
                info_dict[key] = value
                
            return {
                "status": "active",
                "num_docs": int(info_dict.get("num_docs", 0)),
                "total_indexing_time": float(info_dict.get("total_indexing_time_ms", 0)) / 1000,
                "memory_used": info_dict.get("inverted_sz_mb", 0),
                "indexing_failures": info_dict.get("indexing_failures", 0)
            }
                
        except redis.ResponseError as e:
            if "unknown command" in str(e).lower():
                return {"status": "RediSearch module not loaded"}
            elif "unknown index name" in str(e).lower():
                return {"status": "index not found"}
            else:
                return {"status": f"error: {str(e)}"}
        except Exception as e:
            return {"status": f"error: {str(e)}"}

    def get_instance_stats(self, config: Dict, instance_num: int) -> Dict:
        """Get statistics for a single Redis instance"""
        instance_name = f"Redis Instance {instance_num} ({config['host']})"
        start_time = time.time()

        try:
            client = redis.Redis(
                host=config['host'],
                port=config['port'],
                password=config['password'],
                decode_responses=True,
                socket_timeout=30,
                socket_connect_timeout=5,
                socket_keepalive=True,
                retry_on_timeout=True
            )

            # Test connection
            client.ping()

            # Define indexes to check
            indexes = {
                "POI Index": "poi-idx",
                "Entry POI Index": "entry-poi-idx",
                "EVSE Power Index": "evse-power-idx"
            }

            stats = {}
            for name, idx in indexes.items():
                stats[name] = self.check_index_info(client, idx)

            return {
                "instance": instance_name,
                "stats": stats,
                "status": "success",
                "time_taken": time.time() - start_time
            }

        except redis.ConnectionError as e:
            return {
                "instance": instance_name,
                "error": f"Connection error: {str(e)}",
                "status": "error",
                "time_taken": time.time() - start_time
            }
        except redis.AuthenticationError:
            return {
                "instance": instance_name,
                "error": "Authentication failed",
                "status": "error",
                "time_taken": time.time() - start_time
            }
        except Exception as e:
            return {
                "instance": instance_name,
                "error": f"Unexpected error: {str(e)}",
                "status": "error",
                "time_taken": time.time() - start_time
            }
        finally:
            try:
                client.close()
            except:
                pass

    def get_key_statistics(self) -> List[Dict]:
        """Get statistics for all Redis instances"""
        results = []
        for i, config in enumerate(self.redis_configs):
            result = self.get_instance_stats(config, i+1)
            results.append(result)
        return results

    def send_teams_notification(self, stats: List[Dict], build_number: str) -> bool:
        """Send notification to Microsoft Teams using MessageCard format"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Build the message sections
        sections = []
        
        # Add header section
        sections.append({
            "activityTitle": "🔄 RediSearch Index Statistics",
            "facts": [
                {"name": "Build Number", "value": str(build_number)},
                {"name": "Timestamp", "value": current_time}
            ]
        })
        
        # Process each instance
        for instance_stats in stats:
            facts = []
            facts.append({
                "name": "Execution Time",
                "value": f"{instance_stats['time_taken']:.2f}s"
            })
            
            if instance_stats['status'] == "error":
                facts.append({
                    "name": "Status",
                    "value": f"❌ {instance_stats['error']}"
                })
            else:
                for index_name, index_stats in instance_stats['stats'].items():
                    status_icon = "✅" if index_stats['status'] == "active" else "❌"
                    if index_stats['status'] == "active":
                        value = (f"{status_icon} Documents: {index_stats['num_docs']:,}\n" + 
                                f"Memory Usage: {index_stats['memory_used']}MB\n" +
                                f"Indexing Failures: {index_stats['indexing_failures']}")
                    else:
                        value = f"{status_icon} {index_stats['status']}"
                    
                    facts.append({
                        "name": index_name,
                        "value": value
                    })
            
            sections.append({
                "activitySubtitle": instance_stats['instance'],
                "facts": facts
            })

        message = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "0076D7",
            "summary": "RediSearch Index Statistics",
            "sections": sections
        }

        try:
            response = requests.post(
                self.teams_webhook,
                json=message,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            response.raise_for_status()
            print("Successfully sent notification to Teams")
            return True
        except Exception as e:
            print(f"Failed to send Teams notification: {str(e)}")
            return False

def main():
    if len(sys.argv) < 3:
        print("Usage: script.py <build_number>")
        sys.exit(1)

    build_number = sys.argv[1]

    redis_configs = [
        {
            "host": "192.168.8.226",
            "port": 6379,
            "password": "0ef1sJm19w3OKHiH"
        },
        {
            "host": "192.168.8.211",
            "port": 6379,
            "password": "0ef1sJm19w3OKHiH"
        }
    ]

    try:
        monitor = RedisMonitor(redis_configs)
        start_time = time.time()
        print("Starting RediSearch index monitoring...")
        
        stats = monitor.get_key_statistics()
        
        print("\nRediSearch Index Statistics:")
        for instance_stats in stats:
            print(f"\n{instance_stats['instance']} "
                  f"(took {instance_stats['time_taken']:.2f}s):")
            if instance_stats['status'] == "error":
                print(f"Error: {instance_stats['error']}")
            else:
                for index_name, index_stats in instance_stats['stats'].items():
                    status = "✅" if index_stats['status'] == "active" else "❌"
                    print(f"\n{status} {index_name}:")
                    for key, value in index_stats.items():
                        print(f"  - {key}: {value}")

        print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")
        monitor.send_teams_notification(stats, build_number)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()