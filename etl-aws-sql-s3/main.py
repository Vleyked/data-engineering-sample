from booking.bookingsingestion import BookingsIngestion
from customer.customersingestion import CustomersIngestion
from vehicle.vehiclesingestion import VehiclesIngestion


if __name__ == "__main__":
    ci = CustomersIngestion()
    vi = VehiclesIngestion()
    bi = BookingsIngestion()

    # ci.customersDf_full_s3Load()
    ci.customersDf_delta_s3Load()
    # vi.parkingAreasDf_full_s3Load()
    vi.parkingAreasDf_delta_s3Load()
    # vi.vehiclesDf_full_s3Load()
    vi.vehiclesDf_delta_s3Load()
    vi.vehicleEventsDf_delta_s3Load()
    # bi.bookingsDf_full_s3Load()
    bi.bookingsDf_delta_s3Load()
