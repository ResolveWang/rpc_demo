import grpc

import pi_pb2
import pi_pb2_grpc


def main():
    channel = grpc.insecure_channel('localhost:8080')
    client = pi_pb2_grpc.PiCalculatorStub(channel)
    for i in range(0, 1000):
        try:
            print('pi(%d) =' % i, client.Calc(pi_pb2.PiRequest(n=i), timeout=5).value)
        except grpc.RpcError as e:
            print(e.code())
            print(e.details())


if __name__ == '__main__':
    main()