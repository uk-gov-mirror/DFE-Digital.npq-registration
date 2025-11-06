class Exporters::Contracts
  FIELD_NAMES =
    %w[
      lead_provider_name
      course_identifier
      recruitment_target
      per_participant
      service_fee_installments
      special_course
      monthly_service_fee
    ].freeze
  # technically service_fee_installments and monthly_service_fee are not used, but have been uploaded previously
  # monthly_service_fee, output_payment_percentage, number_of_payment_periods, service_fee_installments and service_fee_percentage will be removed in CPDNPQ-2927

  def initialize(cohort:)
    @cohort = cohort
  end

  def call
    CSV.generate(encoding: "utf-8") do |csv|
      csv << FIELD_NAMES
      contract_templates.each do |record|
        csv << FIELD_NAMES.map { |field| attribute(field, record) }
      end
    end
  end

private

  attr_reader :cohort

  def attribute(field, record)
    case field
    when "monthly_service_fee"
      record.attributes[field] || 0
    else
      record.attributes[field]
    end
  end

  def contract_templates
    statements_lead_provider_courses = Statement
      .joins(contracts: [{ statement: :lead_provider }, :course])
      .where(cohort:, output_fee: true)
      .group(:lead_provider_id, :course_id)
      .order(:lead_provider_id, :course_id)
      .count

    statements_lead_provider_courses.map do |values|
      lead_provider_id, course_id = values[0]

      ContractTemplate
        .joins(contracts: [{ statement: :lead_provider }, :course])
        .where(statements: { lead_provider_id: }, contracts: { course_id: })
        .where("MAKE_DATE(statements.year, statements.month, 1) <= DATE_TRUNC('month', CURRENT_DATE)")
        .order("statements.year desc", "statements.month desc")
        .limit(1)
        .select(
          "lead_providers.name as lead_provider_name",
          "courses.identifier as course_identifier",
          :recruitment_target,
          :per_participant,
          :service_fee_installments,
          :special_course,
          :monthly_service_fee,
        ).first
    end
  end
end
