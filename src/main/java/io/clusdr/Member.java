package io.clusdr;

import java.util.Objects;

/** A cluster member as returned by {@link Cluster#members()} / {@link Cluster#leader()}. */
public final class Member {
  private final String id;
  private final String address;
  private final String status;
  private final boolean leader;
  private final String role;

  public Member(String id, String address, String status, boolean leader, String role) {
    this.id = id == null ? "" : id;
    this.address = address == null ? "" : address;
    this.status = status == null ? "" : status;
    this.leader = leader;
    this.role = role == null || role.isEmpty() ? "voter" : role;
  }

  public String id() {
    return id;
  }

  public String address() {
    return address;
  }

  /** Liveness: {@code alive} or {@code dead}. A left id is gone from {@link Cluster#members()}. */
  public String status() {
    return status;
  }

  public boolean leader() {
    return leader;
  }

  public String role() {
    return role;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Member m)) {
      return false;
    }
    return leader == m.leader
        && id.equals(m.id)
        && address.equals(m.address)
        && status.equals(m.status)
        && role.equals(m.role);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, address, status, leader, role);
  }

  @Override
  public String toString() {
    return "Member{id="
        + id
        + ", address="
        + address
        + ", status="
        + status
        + ", leader="
        + leader
        + ", role="
        + role
        + "}";
  }
}
